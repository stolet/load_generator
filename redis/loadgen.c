#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <errno.h>

#define MAX_BUF 1024
#define MIN_KEY 1
#define MAX_KEY 10000
#define MAX_CONNS 1000
#define DEFAULT_VAL_SIZE 64
#define DEFAULT_SET_RATIO 1
#define DEFAULT_GET_RATIO 9
#define DEFAULT_MAX_PENDING_REQS 1

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

////// READ ONLY //////
struct config
{
  // IP address of redis server
  const char *ip;
  // Port of redis server
  int port;
  // How long to run loadgen for
  int duration;
  // How many SET requests to send compared to GET
  int set_ratio;
  // How many GET requests to send compared to SET
  int get_ratio;
  // Number of threads to start
  int ncores;
  // Number of connections to open per core
  int nconns;
  // Max number of pending requests for each conenction
  int max_pending;
  // Distribution to use to sample keys
  enum dist dist;
};
///////////////////////

struct conn
{
  // Socket fd for connection
  int fd;
  // Connection status from conn_state
  int status;
  // Number of pending requests sent to redis
  int pending;
  // Buffer used to transmit requests to redis
  char *tx_buf;
  // Buffer used to received requests from redis
  char *rx_buf;
  // Array of unique set keys sent by this connection
  int *set_keys;
  // Number of unique set keys sent by this connection
  int set_keys_n;
  // Index that determines if next key is a SET or GET
  int ratio_i;
  // Max value for ratio i before it loops back to 0
  int ratio_max_i;
  // Sequential counter used to determine next key in SEQUENTIAL distribution
  int sequential_counter;
};

struct core
{
  // Core id
  int id;
  // Epoll fd used to wait for events
  int ep;
  // Configuration parameters
  struct config *conf;
  // Array of connections for this core
  struct conn *conns;
  // Pthread used to start thread
  pthread_t pthread;
};

struct loadgen
{
  // Configuration for load generator
  struct config *conf;
  // Start time for load generator
  struct timeval start_time;
  // Array of cores started by pthread_create
  struct core *cores;
};

/*****************************************************************************/
/******************************** Parsing CMDL *******************************/

void print_usage(const char *prog_name)
{
  printf("Usage: %s [options]                                                             \n"
         "General options:                                                                \n"
         "  --host     <ADDR>  Redis server ip address                                    \n"
         "  --port     <INT>   Redis server listening port                                \n"
         "  --duration <INT>   Number of seconds to run                                   \n"
         "                                                                                \n"
         "Load options:                                                                   \n"
         "  --nconns   <INT>  Number of connections per core                              \n"
         "  --ncores   <INT>  Number of cores                                             \n"
         "  --pending  <INT>  Max number of requests per connection                       \n"
         "                                                                                \n"
         "Key options:                                                                    \n"
         "  --ratio        <SET:GET>  Ratio of SET and GET commands [default: %d:%d]      \n"
         "  --distribution <dist>     Distribution to generate keys [default: uniform]    \n"
         "    Options: uniform, zipfian, sequential                                       \n",
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
      {"pending", required_argument, 0, 0},
      {"ratio", required_argument, 0, 0},
      {"distribution", required_argument, 0, 0},
      {0, 0, 0, 0}};

  while ((opt = getopt_long(argc, argv, "", options, &option_index)) != -1)
  {
    if (opt == 0)
    {
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
    else if (strcmp(options[option_index].name, "pending") == 0)
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
  conf->max_pending = DEFAULT_MAX_PENDING_REQS;
  conf->dist = UNIFORM;
}

int init_conn(struct config *conf, struct conn *con)
{
  int *set_keys;
  char *tx_buf, *rx_buf;

  tx_buf = malloc(MAX_BUF);
  if (tx_buf == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc tx_buf\n");
    return -1;
  }

  rx_buf = malloc(MAX_BUF);
  if (rx_buf == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc rx_buf\n");
    return -1;
  }

  set_keys = calloc(MAX_KEY, sizeof(int));
  if (set_keys == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc set_keys\n");
    return -1;
  }

  con->fd = -1;
  con->status = CONN_DISCONNETED;
  con->pending = 0;
  con->tx_buf = tx_buf;
  con->rx_buf = rx_buf;
  con->set_keys = set_keys;
  con->set_keys_n = 0;
  con->ratio_i = 0;
  con->ratio_max_i = conf->set_ratio + conf->get_ratio;
  con->sequential_counter = MIN_KEY;

  return 0;
}

int init_core(struct core *cor, struct config *conf)
{
  int i, ret, ep;
  struct conn *conns;


  conns = calloc(conf->nconns, sizeof(struct conn));
  if (conns == NULL)
  {
    fprintf(stderr, "init_core: failed to allocate connections\n");
    return -1;
  }

  for (i = 0; i < conf->nconns; i++)
  {
    ret = init_conn(conf, &conns[i]);
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

  cor->conf = conf;
  cor->conns = conns;
  cor->ep = ep;
  cor->id = next_core_id++;
  return 0;
}

int init_loadgen(struct loadgen *lg, struct config *conf)
{
  struct core *cores;

  lg->conf = conf;
  gettimeofday(&lg->start_time, NULL);

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

int sample_sequential(struct conn *con)
{
  return con->sequential_counter++ % MAX_KEY;
}

int generate_key(struct core *cor, struct conn *con)
{
  switch (cor->conf->dist)
  {
  case UNIFORM:
    return sample_uniform(MIN_KEY, MAX_KEY);
  case ZIPFIAN:
    return sample_zipf(1.0, MAX_KEY);
  case SEQUENTIAL:
    return sample_sequential(con);
  default:
    return sample_uniform(MIN_KEY, MAX_KEY);
  }
}

/*****************************************************************************/

/*****************************************************************************/
/********************************** Data path ********************************/

int redis_set(int fd, char *key, size_t key_len, char *val, size_t val_len)
{
  char buffer[MAX_BUF];

  int len = snprintf(buffer, sizeof(buffer),
                     "*3\r\n$3\r\nSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                     key_len, key, val_len, val);

  if (len < 0)
  {
    perror("redis_set: snprintf failed for SET command");
    return -1;
  }

  if (len > MAX_BUF)
  {
    perror("redis_set: buffer too small for SET command");
    return -1;
  }

  int ret = send(fd, buffer, len, 0);
  if (ret == -1)
  {
    perror("redis_set: failed to send SET command");
    return -1;
  }

  return 0;
}

int redis_get(int fd, char *key, size_t key_len)
{
  char buffer[MAX_BUF];

  int len = snprintf(buffer, sizeof(buffer),
                     "*2\r\n$3\r\nGET\r\n$%zu\r\n%s\r\n", key_len, key);

  if (len < 0)
  {
    perror("redis_get: snprintf failed for SET command");
    return -1;
  }

  if (len > MAX_BUF)
  {
    perror("redis_get: buffer too small SET command");
    return -1;
  }

  int ret = send(fd, buffer, len, 0);
  if (ret == -1)
  {
    perror("redis_get: failed to send GET command");
    return -1;
  }

  return 0;
}

int redis_parse_response(char *buf)
{
  int ret, str_len;
  char *str_start;
  // Parse first character to determine type of response
  switch (buf[0])
  {
    case '+':
      // Simple string: SET
      printf("SET: %s\n", buf);
      break;
    case '$':
      // Bulk string: GET
      ret = sscanf(buf + 1, "%d\r\n", &str_len);
      if (ret != 1)
      {
        fprintf(stderr, "redis_parse_response: failed to get str_len\n");
        return -1;
      }

      // Sent a GET for nonexistent key
      if (str_len == -1)
      {
        printf("GET: nonexistent key\n");
        return 0;
      }

      // Find the start of the bulk string
      str_start = strstr(buf, "\r\n") + 2;

      // Check if str_len + trailing \r\n matches bulk string
      if (strlen(str_start) < (size_t) (str_len + 2))
      {
        fprintf(stderr, "redis_parse_response: "
            "size of bulk string is incorrect\n");
      }

      // Add trailing 0 so we can print string
      printf("GET: %s\n", str_start);
      break;
    case '-':
      // Error
      fprintf(stderr, "redis_parse_response: incorrect command\n");
      return -1;
      break;
    default:
      fprintf(stderr, "redis_parse_response: unknow response type\n");
      return -1;
      break;
  }

  return 0;
}

int redis_recv(struct conn *con)
{
  int ret, len;
  char buf[MAX_BUF];

  while (con->pending > 0)
  {
    len = recv(con->fd, buf, sizeof(buf), 0);
    if (len < 0)
    {
      perror("redis_recv: error when calling recv");
    }

    // Add null terminator to string
    buf[len] = '\0';
    ret = redis_parse_response(buf);
    if (ret < 0)
    {
      fprintf(stderr, "redis_recv: failed to parse redis response\n");
      return -1;
    }

    con->pending--;
  }

  return 0;

}

int redis_send(struct core *cor, struct conn *con)
{
  int ret;
  int key, key_len;
  char key_str[MAX_BUF];
  char set_val[DEFAULT_VAL_SIZE];

  key = generate_key(cor, con);
  key_len = snprintf(key_str, MAX_BUF, "%d", key);

  while (con->pending < cor->conf->max_pending)
  {
    if (con->ratio_i < cor->conf->set_ratio)
    {
      ret = redis_set(con->fd, key_str, key_len, set_val, DEFAULT_VAL_SIZE);
      if (ret < 0)
      {
        fprintf(stderr, "redis_send: sending SET command failed\n");
        return -1;
      }
    }
    else
    {
      ret = redis_get(con->fd, key_str, key_len);
      if (ret < 0)
      {
        fprintf(stderr, "redis_send: sending GET command failed\n");
        return -1;
      }
    }

    con->ratio_i = (con->ratio_i + 1) % con->ratio_max_i;
    con->pending++;
  }

  return 0;

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
      ret = getsockopt(con->fd, SOL_SOCKET, SO_ERROR, &status, &slen);
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
        return -1;
      }
    }

    // Check if we received a response
    if (evs[i].events & EPOLLIN)
    {
      ret = redis_recv(con);
      if (ret < 0)
      {
        fprintf(stderr, "handle_events: redis_recv failed\n");
      }
    }

    // Send commands to redis while pending has not reached max
    if (con->pending < cor->conf->max_pending)
    {
      ret = redis_send(cor, con);
      if (ret < 0)
      {
        fprintf(stderr, "handle_events: redis_send faield\n");
        return -1;
      }
    }
  }

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/******************************** Control Path *******************************/

int redis_connect(struct core *cor, struct conn *con)
{
  int conn_fd, flags, ret, port;
  const char *ip;
  struct epoll_event ev;
  struct sockaddr_in server_addr;

  ip = cor->conf->ip;
  port = cor->conf->port;

  conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (conn_fd < 0)
  {
    perror("Socket creation failed");
    return -1;
  }

  // Get current flags for socket
  flags = fcntl(conn_fd, F_GETFL, 0);
  if (flags < 0)
  {
    perror("redis_connect: failed to get socket flgs");
    return -1;
  }

  // Set socket to nonblocking
  flags |= O_NONBLOCK;
  ret = fcntl(conn_fd, F_SETFL, 0);
  if (ret < 0)
  {
    perror("redis_connect: failed to set socket to nonblocking");
    return -1;
  }

  // Add socket to epoll
  ret = epoll_ctl(cor->ep, EPOLL_CTL_ADD, conn_fd, &ev);
  if (ret < 0)
  {
    perror("redis_connect: failed to add socket to epoll");
    return -1;
  }

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);

  ret = connect(conn_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
  if (ret == 0)
  {
    con->status = CONN_CONNECTED;
  }
  else if (ret < 0  && errno == EINPROGRESS)
  {
    con->status = CONN_CONNECTING;
  }
  else
  {
    perror("redis_connect: failed to connect");
    return -1;
  }

  con->fd = conn_fd;
  return 0;
}

int redis_connect_all(struct core *cor)
{
  int i, ret;

  for (i = 0; i < cor->conf->nconns; i++)
  {
    ret = redis_connect(cor, &cor->conns[i]);
    if (ret < 0)
    {
      fprintf(stderr, "redis_conenct_all: failed to connect to redis\n");
      return -1;
    }
  }

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/****************************** Multithreading *******************************/

void *run_core(void *arg)
{
  int nevs, ret;
  struct epoll_event *evs;
  struct core *cor = arg;

  // Open all connections for this core (nonblocking)
  ret = redis_connect_all(cor);
  if (ret < 0)
  {
    fprintf(stderr, "run_core: failed to open all connections\n");
    abort();
  }

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
      abort();
    }

    // Process events
    ret = handle_events(cor, evs, ret);
    if (ret < 0)
    {
      fprintf(stderr, "run_core: error when handling events\n");
      abort();
    }
  }
}

int start_cores(struct loadgen *lg)
{
  int i, ret;

  for (i = 0; i < lg->conf->ncores; i++)
  {
    ret = pthread_create(&lg->cores[i].pthread, NULL,
                         run_core, (void *) &lg->cores[i]);

    if (ret != 0)
    {
      fprintf(stderr, "start_cores: pthread_create failed\n");
      return -1;
    }
  }

  return 0;
}

int stop_cores(struct loadgen *lg)
{
  printf("stop_cores: duration=%d", lg->conf->duration);
  return 0;
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
  while (get_time_ms() < end_time)
  {
  }

  // Get statistics

  // Cleanup
  stop_cores(&lg);

  return 0;
}
