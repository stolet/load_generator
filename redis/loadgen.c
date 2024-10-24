#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <sys/epoll.h>

#define MAX_KEY_BUF 64
#define MAX_COMMAND_BUF 1024
#define MIN_KEY 1
#define MAX_KEY 10000
#define MAX_CONNS 1000
#define DEFAULT_VAL_SIZE 64
#define DEFAULT_SET_RATIO 1
#define DEFAULT_GET_RATIO 9

static int next_core_id = 0;

enum dist
{
  UNIFORM,
  ZIPFIAN,
  SEQUENTIAL
};

enum conn_state
{
  CONN_DISCONNETED,
  CONN_CONNECTING,
  CONN_CONNECTED
};

struct config
{
  const char *ip;
  int port;
  int duration;
  int set_ratio;
  int get_ratio;
  int ncores;
  int nconns;
  enum dist dist;
};

struct conn
{
  int fd;
  int status;
  char *tx_buf;
  char *rx_buf;
};

struct core
{
  int id;
  int ep;
  struct config *conf;
  struct conn *conns;
  pthread_t pthread;
};

struct loadgen
{
  struct config *conf;
  struct timeval start_time;
  int sequential_counter;
  int *set_keys;
  int set_key_count;
  struct core *cores;
};

/*****************************************************************************/
/******************************** Parsing CMDL *******************************/

void print_usage(const char *prog_name)
{
  printf("Usage: %s [options]                                                          \n"
         "General options:                                                             \n"
         "  --host <ADDR>          Redis server ip address                             \n"
         "  --port <INT>           Redis server listening port                         \n"
         "  --duration <INT>       Number of seconds to run                            \n"
         "                                                                             \n"
        "Load options:                                                                 \n"
         "  --nconns <INT>         Number of connections per core                      \n"
         "  --ncores <INT>         Nunber of cores                                     \n"
         "                                                                             \n"
         "Key options:                                                                 \n"
         "  --ratio <SET:GET>      Ratio of SET and GET commands [default: %d:%d]      \n"
         "  --distribution <dist>  Distribution to generate keys [default: uniform]    \n"
         "    Options: uniform, zipfian, sequential                                    \n",
         prog_name, DEFAULT_SET_RATIO, DEFAULT_GET_RATIO);
}

int parse_args(int argc, char **argv, struct config *conf)
{
  int opt;
  int option_index = 0;

  static struct option options[] = {
      {"host", required_argument, 0, 0},
      {"port", required_argument, 0, 0},
      {"duration", required_argument, 0, 0},
      {"nconns", required_argument, 0, 0},
      {"ncores", required_argument, 0, 0},
      {"ratio", required_argument, 0, 0},
      {"distribution", required_argument, 0, 0},
      {0, 0, 0, 0}
  };

  while ((opt = getopt_long(argc, argv, "", options, &option_index)) != -1)
  {
    if (opt == 0)
    { // Long options have been detected
      if (strcmp(options[option_index].name, "host") == 0)
      {
        conf->ip = optarg;
      }
      else if (strcmp(options[option_index].name, "port") == 0)
      {
        conf->port = atoi(optarg);
      }
      else if (strcmp(options[option_index].name, "ratio") == 0)
      {
        char *token = strtok(optarg, ":");
        if (!token)
        {
          print_usage(argv[0]);
          return -1;
        }

        conf->set_ratio = atoi(token);

        token = strtok(NULL, ":");
        if (!token)
        {
          print_usage(argv[0]);
          return -1;
        }

        conf->get_ratio = atoi(token);
      }
      else if (strcmp(options[option_index].name, "distribution") == 0)
      {
        if (strcmp(optarg, "uniform") == 0)
        {
          conf->dist = UNIFORM;
        }
        else if (strcmp(optarg, "zipfian") == 0)
        {
          conf->dist = ZIPFIAN;
        }
        else if (strcmp(optarg, "sequential") == 0)
        {
          conf->dist = SEQUENTIAL;
        }
        else
        {
          print_usage(argv[0]);
          return -1;
        }
      }
      else if (strcmp(options[option_index].name, "duration") == 0)
      {
        conf->duration = atoi(optarg);
      }
    }
    else if (strcmp(options[option_index].name, "nconns") == 0)
    {
      conf->nconns = atoi(optarg);
    }
    else if (strcmp(options[option_index].name, "ncores") == 0)
    {
      conf->ncores = atoi(optarg);
    }
    else
    {
      print_usage(argv[0]);
      return -1;
    }
  }
  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/************************* Initialization procedures *************************/

void init_config(struct config *conf)
{
  conf->ip = NULL;
  conf->port = 0;
  conf->duration = 0;
  conf->set_ratio = DEFAULT_SET_RATIO;
  conf->get_ratio = DEFAULT_GET_RATIO;
  conf->dist = UNIFORM;
}

int init_conn(struct conn *con)
{
  char *tx_buf, *rx_buf;

  con->fd = -1;
  con->status = CONN_DISCONNETED;

  tx_buf = malloc(MAX_COMMAND_BUF);
  if (tx_buf == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc tx_buf\n");
    return -1;
  }
  con->tx_buf = tx_buf;

  rx_buf = malloc(MAX_COMMAND_BUF);
  if (rx_buf == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc rx_buf\n");
    return -1;
  }
  con->rx_buf = rx_buf;

  return 0;
}

int init_core(struct core *cor, struct config *conf)
{
  int i, ret, ep;
  struct conn *conns;

  cor->conf = conf;

  conns = calloc(conf->nconns, sizeof(struct conn));
  if (conns == NULL)
  {
    fprintf(stderr, "init_core: failed to allocate connections\n");
    return -1;
  }
  cor->conns = conns;

  for (i = 0; i < conf->nconns; i++)
  {
    ret = init_conn(&conns[i]);
    if (ret < 0)
    {
      fprintf(stderr, "init_core: failed to initialize connection\n");
      return -1;
    }
  }

  ep = epoll_create(1);
  if (ep < 0)
  {
    fprintf(stderr, "init_core: epoll_create failed\n");
    return -1;
  }
  cor->ep = ep;

  cor->id = next_core_id++;
}

int init_loadgen(struct loadgen *lg, struct config *conf)
{
  int *set_keys;
  struct core *cores;

  lg->conf = conf;
  gettimeofday(&lg->start_time, NULL);
  lg->sequential_counter = MIN_KEY;
  lg->set_key_count = 0;

  set_keys = malloc(MAX_KEY * sizeof(int));
  if (set_keys == NULL)
  {
    fprintf(stderr, "init_loadgen: failed to malloc set_keys\n");
    return -1;
  }
  lg->set_keys = set_keys;

  cores = calloc(conf->ncores, sizeof(struct core));
  for (int i = 0; i < conf->ncores; i++)
  {
    init_core(&cores[i], conf);
  }

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/******************************* Key generation ******************************/

int sample_uniform(int min, int max)
{
  return rand() % (max - min + 1) + min;
}

int sample_zipf(int s, int n)
{
  int i;
  double p = (rand() / (RAND_MAX + 1.0));
  double sum_prob = 0.0;

  for (i = 1; i <= n; i++)
  {
    sum_prob += (1.0 / pow(i, s));
    if (p <= sum_prob)
    {
      return i;
    }
  }

  return n;
}

int sample_sequential(struct loadgen *lg)
{
  return lg->sequential_counter++ % MAX_KEY;
}

int generate_key(struct loadgen *lg)
{
  switch (lg->conf->dist)
  {
  case UNIFORM:
    return sample_uniform(MIN_KEY, MAX_KEY);
  case ZIPFIAN:
    return sample_zipf(1.0, MAX_KEY);
  case SEQUENTIAL:
    return sample_sequential(lg);
  default:
    return sample_uniform(MIN_KEY, MAX_KEY);
  }
}

/*****************************************************************************/

/*****************************************************************************/
/********************************** Data path ********************************/

void redis_set(int fd, const char *key, size_t key_len,
               const char *val, size_t val_len)
{
  char buffer[MAX_COMMAND_BUF];

  int len = snprintf(buffer, sizeof(buffer),
                     "*3\r\n$3\r\nSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                     key_len, key, val_len, val);

  if (len < 0)
  {
    perror("snprintf failed for SET command");
    return;
  }

  if (len > MAX_COMMAND_BUF)
  {
    perror("buffer too small for SET command");
  }

  int ret = send(fd, buffer, len, 0);
  if (ret == -1)
  {
    perror("Failed to send SET command");
  }
}

void redis_get(int fd, const char *key, size_t key_len)
{
  char buffer[MAX_COMMAND_BUF];

  int len = snprintf(buffer, sizeof(buffer),
                     "*2\r\n$3\r\nGET\r\n$%zu\r\n%s\r\n", key_len, key);

  if (len < 0)
  {
    perror("snprintf failed for SET command");
    return;
  }

  if (len > MAX_COMMAND_BUF)
  {
    perror("buffer too small SET command");
  }

  int ret = send(fd, buffer, len, 0);
  if (ret == -1)
  {
    perror("Failed to send GET command");
  }
}

int handle_events(struct core *cor, struct epoll_event *evs, int n)
{
  int i, ret, status;
  socklen_t slen;
  struct conn *con;

  for (i = 0; i < n; i++)
  {
    con = evs[i].data.ptr;

    // Check if connection was established
    if (con->status == CONN_CONNECTING)
    {
      // Use getsockopt to query connection status
      slen = sizeof(status);
      ret = getsockopt(con, SOL_SOCKET, SO_ERROR, &status, &slen);
      if (ret < 0)
      {
        fprintf(stderr, "handle_events: getsockopt failed\n");
        return -1;
      }

      // If status is 0 we connected successfully, if not there was an error
      if (status == 0)
      {
        con->status = CONN_CONNECTED;
      }
      else
      {
        fprintf(stderr, "handle_events: failed status from getsockopt\n");
      }
    }

    // Check if we received a response
    if (evs[i].events & EPOLLIN)
    {
      redis_recv(cor, con);
    }

    // Send commands to redis
    redis_send(cor, con);
  }
}

void generate_load(struct loadgen *lg)
{
  int i, total_requests, key, key_len;
  char key_str[MAX_KEY_BUF];
  const char set_val[DEFAULT_VAL_SIZE];

  total_requests = lg->conf->set_ratio + lg->conf->get_ratio;

  while (1)
  {
    for (i = 0; i < total_requests; i++)
    {
      key = generate_key(lg);
      key_len = snprintf(key_str, MAX_KEY_BUF, "%d", key);

      if (i < lg->conf->set_ratio)
      {
        redis_set(lg->conn_fd, key_str, key_len,
                  set_val, DEFAULT_VAL_SIZE);
      }
      else
      {
        redis_get(lg->conn_fd, key_str, key_len);
      }
    }
  }
}

/*****************************************************************************/

/*****************************************************************************/
/******************************** Control Path *******************************/

int redis_connect(const char *ip, int port)
{
  int conn_fd;
  struct sockaddr_in server_addr;

  conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (conn_fd < 0)
  {
    perror("Socket creation failed");
    return -1;
  }

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);

  if (connect(conn_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
  {
    perror("Connection failed");
    return -1;
  }

  return conn_fd;
}

int redis_connect_all(struct core *cor)
{

}

/*****************************************************************************/

/*****************************************************************************/
/****************************** Multithreading *******************************/

int start_core(void *arg)
{
  int nevs, ret;
  struct epoll_event *evs;
  struct core *cor = arg;

  // Open all connections for this core (nonblocking)
  redis_connect_all(cor);

  // Wait for connection and recv events
  nevs = cor->conf->nconns;
  evs = calloc(nevs, sizeof(struct epoll_event));
  while (1)
  {
    // Check if there are any new events
    ret = epoll_wait(cor->ep, evs, nevs, -1);
    if (ret < 0)
    {
      fprintf(stderr, "start_core: epoll wait failed\n");
      return -1;
    }

    // Process events
    handle_events(cor, evs, ret);
  }
}

int start_cores(struct loadgen *lg)
{
  int i, ret;

  for (i = 0; i < lg->conf->ncores; i++)
  {
    ret = pthread_create(&lg->cores[i].pthread, NULL,
        start_core, &lg->cores[i]);

    if (ret != 0)
    {
      fprintf(stderr, "start_cores: pthread_create failed\n");
      return -1;
    }
  }
}

int stop_cores(struct loadgen *lg)
{

}

/*****************************************************************************/

/*****************************************************************************/
/************************************ Time ***********************************/

long get_time_ms()
{
  struct timeval now;
  gettimeofday(&now, NULL);
  return now.tv_sec * 1000 + now.tv_usec / 1000;
}

/*****************************************************************************/

int main(int argc, char **argv)
{
  struct loadgen lg;
  struct config conf;

  srand(time(NULL));

  // Get configuration from args
  init_config(&conf);
  int ret = parse_args(argc, argv, &conf);
  if (ret < 0)
  {
    fprintf(stderr, "main: failed to parse options\n");
    exit(-1);
  }

  if (conf.ip == NULL || conf.port == 0 || conf.duration == 0)
  {
    print_usage(argv[0]);
    exit(-1);
  }

  // Initialize load generator
  ret = init_loadgen(&lg, &conf);
  if (ret < 0)
  {
    fprintf(stderr, "main: failed to init loadgen\n");
    exit(-1);
  }

  // Start sending and receiving data
  ret = start_cores(&lg);
  if (ret < 0)
  {
    fprintf(stderr, "main: failed to start cores\n");
    exit(-1);
  }

  // Wait for experiment duration
  long end_time = get_time_ms() + lg.conf->duration * 1000;
  while (get_time_ms() < end_time) {}

  // Get statistics

  // Cleanup
  stop_cores(&lg);

  return 0;
}
