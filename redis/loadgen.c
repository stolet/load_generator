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

#define RES_SSTRING '+'
#define RES_BSTRING '$'
#define RES_ERROR '-'

#define MIN_KEY 0
#define MAX_KEY 10000
#define MAX_LEN_CHARS 8
#define MAX_BUF 16384
#define MAX_VAL_SIZE 16384

#define DEFAULT_RATE 0
#define DEFAULT_VAL_SIZE 64
#define DEFAULT_SET_RATIO 1
#define DEFAULT_GET_RATIO 9
#define DEFAULT_NCONNS 1
#define DEFAULT_NCORES 1
#define DEFAULT_MAX_PENDING 1

#define HIST_BUCKETS 200000
#define HIST_PERCENTILES 5

#define MAX(a, b) a > b ? a : b
#define MIN(a, b) a < b ? a : b

enum parsing_status
{
  PARSING_ERR,
  PARSING_OP,
  PARSING_LEN,
  PARSING_VAL,
  PARSING_COMPLETE
};

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
  // Value size for SET requests
  size_t vsize;
  // Send rate for each connection
  int rate;

  // Distribution to use to sample keys
  enum dist dist;
  // CDF used by the zipf distribution
  double *zipf_cdf;
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
  // Index to tx_buf
  int tx_i;
  // Timestamp when request was sent. Used to compute latency.
  uint64_t *tx_ts;

  // Buffer used to received requests from redis
  char *rx_buf;
  // Number of bytes read into rx_buf
  int rx_nread;
  // Index to rx_buf
  int rx_i;
  // Index to lenstr
  int rx_len_i;
  // Number of expected characters in value for response
  int rx_nval;
  // Flag that signals we parsed \r character
  int rx_rflag;
  // Char array used to hold string representing length of res value
  char rx_lenstr[MAX_LEN_CHARS];
  // Parsing status of rx response
  enum parsing_status rx_status;

  // Tokens used by rate limiter
  uint64_t tokens;
  // Rate at which tokens are added per minute
  uint64_t rate;
  // Maximum capacity of token bucker
  uint64_t tokens_max;
  // Last time the tokens were refilled
  uint64_t last_refill;

  // Total number of requests sent by this connection
  uint64_t nreqs;

  // Array of unique set keys sent by this connection
  int *set_keys;
  // Number of unique set keys sent by this connection
  int set_keys_n;
  // Index that determines if next key is a SET or GET
  int ratio_i;
  // Max value for ratio i before it loops back to 0
  int ratio_max_i;

  // Max value achieved by the sequential counter for set so far
  int seq_counter_set_max;
  // Counter used to determine next key in SEQUENTIAL distribution for SET
  int seq_counter_set;
  // Counter used to determine next key in SEQUENTIAL distribution for GET
  int seq_counter_get;
};

struct core
{
  // Core id
  int id;
  // Epoll fd used to wait for events
  int ep;
  // Loadgen: READ ONLY
  struct loadgen *lg;
  // Array of connections for this core
  struct conn *conns;
  // Pthread used to start thread
  pthread_t pthread;
  // Number of cycles per µ second. Used for timekeeping in rate limit
  uint64_t tsc_per_us;
};

struct tp_metrics
{
  uint64_t ts;
  uint64_t nreqs;
};

struct lat_metrics
{
  double percentiles[HIST_PERCENTILES];
  uint64_t latencies[HIST_PERCENTILES];
};

struct loadgen
{
  // Configuration for load generator
  struct config *conf;
  // Start time for load generator
  struct timeval start_time;
  // Array of cores started by pthread_create
  struct core *cores;
  // Throughput metrics array for each second
  struct tp_metrics *tpmets;
  // Latency metrics
  struct lat_metrics latmets;
  // Index for tp_mets save iteration
  int i_mets;
  // Latency histogram
  uint32_t *lat_hist;
};

// Parsing CMDL
static void print_usage(const char *prog_name);
static int parse_args(int argc, char **argv, struct config *conf);

// Initialization procedures
static int init_config(struct config *conf);
static int init_conn(struct config *conf, struct conn *con,
    uint64_t tsc_per_us);
static int init_core(struct core *cor, struct loadgen *lg,
    struct config *conf, uint64_t tsc_per_us);
static void init_latmets(struct lat_metrics *latmets);
static int init_loadgen(struct loadgen *lg, struct config *conf,
    uint64_t tsc_per_us);

// Key and val generation
static int sample_uniform(int min, int max);
static void gen_zipf_cdf(double *cdf, double s);
static int sample_zipf(double *cdf);
static int sample_sequential_set(struct conn *con);
static int sample_sequential_get(struct conn *con);
static int generate_key_set(struct core *cor, struct conn *con);
static int generate_key_get(struct core *cor, struct conn *con);

// Data path
static int redis_set(struct core *cor, struct conn *con);
static int redis_get(struct core *cor, struct conn *con);
static int redis_send(struct core *cor, struct conn *con);
static int redis_parse_op(struct conn *con);
static int redis_parse_len(struct conn *con);
static int redis_parse_val(struct conn *con);
static int redis_parse_response(struct conn *con);
static int redis_recv(struct core *cor, struct conn *con);
static int handle_events(struct core *cor, struct epoll_event *evs, int n);

// Control path
static int redis_connect(struct core *cor, struct conn *con);
static int redis_connect_all(struct core *cor);

// Multithreading
static void *run_core(void *arg);
static int start_cores(struct loadgen *lg);

// Time
static inline uint64_t get_tsc_calibration();
static inline uint64_t rdtsc(void);
static inline uint64_t get_us_tsc(uint64_t tsc_per_us);
static inline long get_ms();
static inline uint64_t get_nanos(void);

// Metrics
static void tp_metrics_save(struct loadgen *lg, uint64_t now);
static void summarize_metrics(struct loadgen *lg, int i_mets);
static void latency_add(struct loadgen *lg, uint64_t lat);


/*****************************************************************************/
/******************************** Parsing CMDL *******************************/

static void print_usage(const char *prog_name)
{
  printf("Usage: %s [options]                                                             \n"
         "General options:                                                                \n"
         "  --host     <ADDR>  Redis server ip address                                    \n"
         "  --port     <INT>   Redis server listening port                                \n"
         "  --duration <INT>   Number of seconds to run                                   \n"
         "                                                                                \n"
         "Load options:                                                                   \n"
         "  --nconns       <INT>  Number of connections per core                          \n"
         "  --ncores       <INT>  Number of cores                                         \n"
         "  --pending      <INT>  Max number of requests per connection                   \n"
         "  --vsize        <INT>  Value size for set requests                             \n"
         "  --rate         <INT>  Send rate for each connection                           \n"
         "                                                                                \n"
         "Key options:                                                                    \n"
         "  --ratio        <SET:GET>  Ratio of SET and GET commands [default: %d:%d]      \n"
         "  --distribution <dist>     Distribution to generate keys [default: uniform]    \n"
         "    Options: uniform, zipfian, sequential                                       \n",
         prog_name, DEFAULT_SET_RATIO, DEFAULT_GET_RATIO);
}

static int parse_args(int argc, char **argv, struct config *conf)
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
      {"vsize", required_argument, 0, 0},
      {"rate", required_argument, 0, 0},
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
        conf->max_pending = atoi(optarg);
      }
      else if (strcmp(options[option_index].name, "vsize") == 0)
      {
        conf->vsize = atoi(optarg);
      }
      else if (strcmp(options[option_index].name, "rate") == 0)
      {
        conf->rate = atoi(optarg);
      }
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

static int init_config(struct config *conf)
{
  double *zipf_cdf;

  zipf_cdf = calloc(MAX_KEY, sizeof(double));
  if (zipf_cdf == NULL)
  {
    fprintf(stderr, "init_config: failed to allocate zipf_cdf\n");
    return -1;
  }
  gen_zipf_cdf(zipf_cdf, 1);

  conf->ip = NULL;
  conf->port = 0;
  conf->duration = 0;
  conf->nconns = DEFAULT_NCONNS;
  conf->ncores = DEFAULT_NCORES;
  conf->set_ratio = DEFAULT_SET_RATIO;
  conf->get_ratio = DEFAULT_GET_RATIO;
  conf->max_pending = DEFAULT_MAX_PENDING;
  conf->vsize = DEFAULT_VAL_SIZE;
  conf->rate = DEFAULT_RATE;
  conf->dist = UNIFORM;
  conf->zipf_cdf = zipf_cdf;

  return 0;
}

static int init_conn(struct config *conf, struct conn *con,
    uint64_t tsc_per_us)
{
  int *set_keys;
  uint64_t *tx_ts;
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

  tx_ts = calloc(conf->max_pending, sizeof(uint64_t));
  if (tx_ts == NULL)
  {
    fprintf(stderr, "init_conn: failed to malloc tx timestamps\n");
    return -1;
  }

  con->fd = -1;
  con->status = CONN_DISCONNETED;
  con->pending = 0;
  con->tx_buf = tx_buf;
  con->tx_i = 0;
  con->tx_ts = tx_ts;
  con->rx_buf = rx_buf;
  con->rx_nread = 0;
  con->rx_i = 0;
  con->rx_nval = 0;
  con->rx_rflag = 0;
  con->rx_status = PARSING_OP;
  con->tokens = conf->rate;
  con->rate = conf->rate;
  con->tokens_max = conf->rate;
  con->last_refill = get_us_tsc(tsc_per_us);
  con->nreqs = 0;
  con->set_keys = set_keys;
  con->set_keys_n = 0;
  con->ratio_i = 0;
  con->ratio_max_i = conf->set_ratio + conf->get_ratio;
  con->seq_counter_set_max = MIN_KEY;
  con->seq_counter_set = MIN_KEY;
  con->seq_counter_get = MIN_KEY;

  return 0;
}

static int init_core(struct core *cor, struct loadgen *lg,
    struct config *conf, uint64_t tsc_per_us)
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
    ret = init_conn(conf, &conns[i], tsc_per_us);
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

  cor->lg = lg;
  cor->conns = conns;
  cor->ep = ep;
  cor->id = next_core_id++;
  cor->tsc_per_us = tsc_per_us;

  return 0;
}

static void init_latmets(struct lat_metrics *latmets)
{
  int i;
  double percentiles[HIST_PERCENTILES] = {0.50,0.90,0.99,0.999,0.9999};

  for (i = 0; i < HIST_PERCENTILES; i++)
  {
    latmets->latencies[i] = 0;
    latmets->percentiles[i] = percentiles[i];
  }
}

static int init_loadgen(struct loadgen *lg, struct config *conf,
    uint64_t tsc_per_us)
{
  int ret;
  uint32_t *hist;
  struct tp_metrics *tpmets;
  struct core *cores;

  cores = calloc(conf->ncores, sizeof(struct core));
  for (int i = 0; i < conf->ncores; i++)
  {
    ret = init_core(&cores[i], lg, conf, tsc_per_us);
    if (ret < 0)
    {
      fprintf(stderr, "init_loadgen: failed to init core=%d\n", i);
      return -1;
    }
  }

  // Allocate array to hold throughput metrics
  tpmets = calloc(conf->duration, sizeof(struct tp_metrics));
  if (tpmets == NULL)
  {
    fprintf(stderr, "init_loadgen: failed to allocate tp_metrics array\n");
    return -1;
  }

  // Initialize arrays to get latency percentiles
  init_latmets(&lg->latmets);

  // Allocate latency histogram
  hist = calloc(HIST_BUCKETS, sizeof(*hist));
  if (hist == NULL)
  {
    fprintf(stderr, "init_loadgen: failed to allocate latency histogram\n");
  }
  memset(hist, 0, HIST_BUCKETS * sizeof(*hist));

  lg->conf = conf;
  lg->cores = cores;
  gettimeofday(&lg->start_time, NULL);
  lg->tpmets = tpmets;
  lg->i_mets = 0;
  lg->lat_hist = hist;

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/*************************** Key and val generation **************************/

static int sample_uniform(int min, int max)
{
  return rand() % (max - min + 1) + min;
}

static void gen_zipf_cdf(double *cdf, double s)
{
  int i;
  double sum = 0.0, cumu_sum = 0.0;

  // Keys start from 1 so we don't divide by 0
  for (i = 1; i <= MAX_KEY; i++)
  {
    sum += 1.0 / pow(i, s);
  }

  cdf[0] = 0;
  for (i = 1; i <= MAX_KEY; i++)
  {
    cumu_sum += 1.0 / pow(i, s) / sum;
    cdf[i-1] = cumu_sum;
  }
}

static int sample_zipf(double *cdf)
{
  int i;

  // Generate number between 0 and 1
  double r = (double) rand() / RAND_MAX;

  for (i = 1; i < MAX_KEY; i++)
  {
    // Find first rank where r is less than the cdf for rank
    if (r <= cdf[i])
    {
      return i;
    }
  }

  return MAX_KEY;
}

static int sample_sequential_set(struct conn *con)
{
  int sample;

  sample = con->seq_counter_set;

  con->seq_counter_set = (con->seq_counter_set + 1) % MAX_KEY;
  con->seq_counter_set_max = MAX(con->seq_counter_set,
      con->seq_counter_set_max);

  return sample;
}

static int sample_sequential_get(struct conn *con)
{
  int sample;

  sample = con->seq_counter_get;

  con->seq_counter_get = (con->seq_counter_get + 1) %
      con->seq_counter_set_max;

  return sample;
}

static int generate_key_set(struct core *cor, struct conn *con)
{
  switch (cor->lg->conf->dist)
  {
  case UNIFORM:
    return sample_uniform(MIN_KEY, MAX_KEY);
  case ZIPFIAN:
    return sample_zipf(cor->lg->conf->zipf_cdf);
  case SEQUENTIAL:
    return sample_sequential_set(con);
  default:
    return sample_uniform(MIN_KEY, MAX_KEY);
  }
}

static int generate_key_get(struct core *cor, struct conn *con)
{
  switch (cor->lg->conf->dist)
  {
  case UNIFORM:
    return sample_uniform(MIN_KEY, MAX_KEY);
  case ZIPFIAN:
    return sample_zipf(cor->lg->conf->zipf_cdf);
  case SEQUENTIAL:
    return sample_sequential_get(con);
  default:
    return sample_uniform(MIN_KEY, MAX_KEY);
  }
}

/*****************************************************************************/

/*****************************************************************************/
/******************************** Rate limiter *******************************/

static void refill_tokens(struct conn *con, uint64_t tsc_per_us)
{
  uint64_t tokens_up;
  uint64_t now;

  now = get_us_tsc(tsc_per_us);
  if ((now - con->last_refill) >= 1000)
  {
    tokens_up = con->tokens + (con->rate * (now - con->last_refill) / 1000000);

    /* Check if tokens to add > 0 because if rate is too small and
     * the update period is too short we don't add any new tokens
     */
    if (tokens_up > 0)
    {
      con->tokens = MIN(con->tokens_max, tokens_up);
      con->last_refill = now;
    }
  }
}

/*****************************************************************************/

/*****************************************************************************/
/********************************** Data path ********************************/

static int redis_set(struct core *cor, struct conn *con)
{
  int i, ret, key, key_len;
  char key_str[MAX_LEN_CHARS];

  // Generate key from distribution in config
  key = generate_key_set(cor, con);
  key_len = sprintf(key_str, "%d", key);

  // Add SET command
  ret = snprintf(con->tx_buf + con->tx_i, 25, "*3\r\n$3\r\nSET\r\n$");
  con->tx_i += ret;

  // Add key length
  ret = snprintf(con->tx_buf + con->tx_i, MAX_LEN_CHARS, "%d", key_len);
  con->tx_i += ret;

  // Break
  ret = snprintf(con->tx_buf + con->tx_i, 25, "\r\n");
  con->tx_i += ret;

  // Add key string and null terminator
  ret = snprintf(con->tx_buf + con->tx_i, strlen(key_str) + 1, key_str);
  con->tx_i += ret;

  // Break
  ret = snprintf(con->tx_buf + con->tx_i, 25, "\r\n$");
  con->tx_i += ret;

  // Add val length
  ret = snprintf(con->tx_buf + con->tx_i,
      MAX_LEN_CHARS, "%ld", cor->lg->conf->vsize);
  con->tx_i += ret;

  // Break
  ret = snprintf(con->tx_buf + con->tx_i, 25, "\r\n");
  con->tx_i += ret;

  // Add val string
  for (i = 0; (size_t) i < cor->lg->conf->vsize && con->tx_i < MAX_BUF; i++)
  {
    con->tx_buf[con->tx_i] = 'a';
    con->tx_i++;
  }

  // Break
  con->tx_buf[con->tx_i] = '\0';
  con->tx_i++;
  con->tx_buf[con->tx_i] = '\r';
  con->tx_i++;
  con->tx_buf[con->tx_i] = '\n';
  con->tx_i++;

  // Send request to network
  con->tx_ts[con->pending] = get_us_tsc(cor->tsc_per_us);
  ret = send(con->fd, con->tx_buf, con->tx_i, 0);
  con->tx_i = 0;
  if (ret == -1)
  {
    perror("redis_set: failed to send SET command");
    return -1;
  }

  return 0;
}

static int redis_get(struct core *cor, struct conn *con)
{
  int ret, key, key_len;
  char key_str[MAX_LEN_CHARS];

  // Generate key from distribution given in config
  key = generate_key_get(cor, con);
  key_len = sprintf(key_str, "%d", key);

  // Add GET command
  ret = snprintf(con->tx_buf + con->tx_i, 25, "*2\r\n$3\r\nGET\r\n$");
  con->tx_i += ret;

  // Add key length
  ret = snprintf(con->tx_buf + con->tx_i, MAX_LEN_CHARS, "%d", key_len);
  con->tx_i += ret;

  // Break
  ret = snprintf(con->tx_buf + con->tx_i, 25, "\r\n");
  con->tx_i += ret;

  // Add key string and null terminator
  ret = snprintf(con->tx_buf + con->tx_i, strlen(key_str) + 1, key_str);
  con->tx_i += ret;

  // Break
  con->tx_buf[con->tx_i] = '\0';
  con->tx_i++;
  con->tx_buf[con->tx_i] = '\r';
  con->tx_i++;
  con->tx_buf[con->tx_i] = '\n';
  con->tx_i++;

  con->tx_ts[con->pending] = get_us_tsc(cor->tsc_per_us);
  ret = send(con->fd, con->tx_buf, con->tx_i, 0);
  con->tx_i = 0;
  if (ret == -1)
  {
    perror("redis_get: failed to send GET command");
    return -1;
  }

  return 0;
}

static int redis_send(struct core *cor, struct conn *con)
{
  int ret;

  // Refill tokens before sending
  if (con->rate != 0)
    refill_tokens(con, cor->tsc_per_us);

  // Send commands until we reach max pending
  while (con->pending < cor->lg->conf->max_pending)
  {
    if (con->rate != 0 && con->tokens <= 0)
      return 0;

    if (con->ratio_i < cor->lg->conf->set_ratio)
    {
      // Sends SET
      ret = redis_set(cor, con);
      if (ret < 0)
      {
        fprintf(stderr, "redis_send: sending SET command failed\n");
        return -1;
      }
    }
    else
    {
      // Sends GET
      ret = redis_get(cor, con);
      if (ret < 0)
      {
        fprintf(stderr, "redis_send: sending GET command failed\n");
        return -1;
      }
    }

    con->ratio_i = (con->ratio_i + 1) % con->ratio_max_i;
    con->pending++;
    con->tokens--;
  }

  return 0;

}

static int redis_parse_op(struct conn *con)
{
  switch (con->rx_buf[con->rx_i])
  {
    case RES_SSTRING:
      con->rx_status = PARSING_VAL;
      break;
    case RES_BSTRING:
      con->rx_status = PARSING_LEN;
      break;
    case RES_ERROR:
      fprintf(stderr, "redis_parse_op: incorrect command\n");
      con->rx_i += 1;
      con->rx_status = PARSING_ERR;
      return -1;
      break;
    default:
      fprintf(stderr, "redis_parse_op: unknown op=%c\n", con->rx_buf[con->rx_i]);
      con->rx_i += 1;
      con->rx_status = PARSING_ERR;
      return -1;
      break;
  }

  con->rx_i += 1;
  return 0;
}

static int redis_parse_len(struct conn *con)
{
  for (;con->rx_i < con->rx_nread; con->rx_i++)
  {
    switch (con->rx_buf[con->rx_i])
    {
      case '\r':
        con->rx_lenstr[con->rx_len_i] = '\0';
        con->rx_len_i = 0;
        con->rx_rflag = 1;
        break;
      case '\n':
        con->rx_nval = atoi(con->rx_lenstr);
        if (con->rx_nval == -1)
          con->rx_status = PARSING_COMPLETE;
        else
          con->rx_status = PARSING_VAL;

        con->rx_rflag = 0;

        /* Increment rx_i manually since we are returning */
        con->rx_i++;
        return 0;
        break;
      default:
        con->rx_lenstr[con->rx_len_i] = con->rx_buf[con->rx_i];
        con->rx_len_i++;
        break;
    }
  }

  return 0;
}

static int redis_parse_val(struct conn *con)
{
  int rflag;

  rflag = 0;
  for (;con->rx_i < con->rx_nread; con->rx_i++)
  {
    if (con->rx_buf[con->rx_i] == '\r')
    {
      rflag = 1;
    }
    else if (rflag && con->rx_buf[con->rx_i] == '\n')
    {
      // Increment to account for null char
      con->rx_i++;
      con->rx_status = PARSING_COMPLETE;
      return 0;
    }
  }

  return 0;
}

static int redis_parse_response(struct conn *con)
{
  switch (con->rx_status)
  {
    case PARSING_OP:
      redis_parse_op(con);
      break;
    case PARSING_LEN:
      redis_parse_len(con);
      break;
    case PARSING_VAL:
      redis_parse_val(con);
      break;
    case PARSING_ERR:
      fprintf(stderr, "redis_recv: sent unknown command\n");
      abort();
      return -1;
      break;
    default:
      fprintf(stderr, "redis_recv: got unknown parsing status\n");
      return -1;
      break;
  }

  return 0;
}

static int redis_recv(struct core *cor, struct conn *con)
{
  int ret;

  while (con->pending > 0)
  {
    ret = recv(con->fd, con->rx_buf, MAX_BUF, 0);
    if (ret < 0)
    {
      perror("redis_recv: error when calling recv");
      return -1;
    }

    if (ret == 0)
      return 0;

    con->rx_nread = ret;
    con->rx_i = 0;
    while (con->rx_i < con->rx_nread)
    {
      redis_parse_response(con);
      if (con->rx_status == PARSING_COMPLETE)
      {
        con->pending--;
        latency_add(cor->lg,
            get_us_tsc(cor->tsc_per_us) - con->tx_ts[con->pending]);
        __sync_fetch_and_add(&con->nreqs, 1);
        con->rx_status = PARSING_OP;
      }
    }

    con->rx_i = 0;
    con->rx_nread = 0;
  }

  return 0;
}

static int handle_events(struct core *cor, struct epoll_event *evs, int n)
{
  int i, ret, status;
  socklen_t slen;
  struct conn *con;
  for (i = 0; i < n; i++)
  {
    con = evs[i].data.ptr;

    // Check for errors on connection
    if ((evs[i].events & EPOLLERR) != 0)
    {
      fprintf(stderr, "handle_events: error on epoll\n");
      return -1;
    }

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
    if ((evs[i].events & EPOLLIN) == EPOLLIN)
    {
      ret = redis_recv(cor, con);
      if (ret < 0)
      {
        fprintf(stderr, "handle_events: redis_recv failed\n");
      }
    }

    // Send commands to redis while pending has not reached max
    if (con->pending < cor->lg->conf->max_pending)
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

static int redis_connect(struct core *cor, struct conn *con)
{
  int conn_fd, flags, ret, port;
  const char *ip;
  struct epoll_event ev;
  struct sockaddr_in server_addr;

  ip = cor->lg->conf->ip;
  port = cor->lg->conf->port;

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
  ev.data.ptr = con;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR;
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

static int redis_connect_all(struct core *cor)
{
  int i, ret;

  for (i = 0; i < cor->lg->conf->nconns; i++)
  {
    ret = redis_connect(cor, &cor->conns[i]);
    if (ret < 0)
    {
      fprintf(stderr, "redis_connect_all: failed to connect to redis\n");
      return -1;
    }
  }

  return 0;
}

static int redis_close(struct conn *con)
{
  int ret;

  ret = close(con->fd);
  if (ret < 0)
  {
    fprintf(stderr, "redis_close: failed to close socket\n");
    return -1;
  }

  return 0;
}

static int redis_close_all(struct loadgen *lg)
{
  int i, j, ret;

  for (i = 0; i < lg->conf->ncores; i++)
  {
    for (j = 0; j < lg->conf->nconns; j++)
    {
      ret = redis_close(&lg->cores[i].conns[j]);
      if (ret < 0)
      {
        fprintf(stderr, "redis_close_all: failed to close conn to redis\n");
        return -1;
      }
    }

  }

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/****************************** Multithreading *******************************/

static void *run_core(void *arg)
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
  nevs = cor->lg->conf->nconns;
  evs = calloc(nevs, sizeof(struct epoll_event));
  while (1)
  {
    // Check if there are any new events
    ret = epoll_wait(cor->ep, evs, nevs, 0);
    if (ret < 0)
    {
      perror("");
      fprintf(stderr, "run_core: epoll wait failed\n");
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

  return NULL;
}

static int start_cores(struct loadgen *lg)
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

static int stop_cores(struct loadgen *lg)
{
  int i, ret;

  for (i = 0; i < lg->conf->ncores; i++)
  {
    ret = pthread_cancel(lg->cores[i].pthread);

    if (ret != 0)
    {
      fprintf(stderr, "stop_cores: pthread_cancel failed ret=%d\n", ret);
      return -1;
    }
  }

  return 0;
}

/*****************************************************************************/

/*****************************************************************************/
/************************************ Time ***********************************/

static inline uint64_t get_tsc_calibration()
{
  struct timespec ts_before, ts_after;
  uint64_t tsc, tsc_per_us;
  double freq;

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_before) != 0)
  {
    fprintf(stderr, "get_tsc_calibration: clock_gettime failed for ts_before\n");
    return 0;
  }

  tsc = rdtsc();
  usleep(10000);
  tsc = rdtsc() - tsc;

  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts_after) != 0)
  {
    fprintf(stderr, "get_tsc_calibration: clock_gettime failed for ts_after\n");
    return 0;
  }

  freq = ((ts_after.tv_sec * 1000000UL) + (ts_after.tv_nsec / 1000)) -
      ((ts_before.tv_sec * 1000000UL) + (ts_before.tv_nsec / 1000));

  tsc_per_us = tsc / freq;
  return tsc_per_us;
}

static inline uint64_t rdtsc(void)
{
    uint32_t eax, edx;
    asm volatile ("rdtsc" : "=a" (eax), "=d" (edx));
    return ((uint64_t) edx << 32) | eax;
}

static inline uint64_t get_us_tsc(uint64_t tsc_per_us)
{
  return rdtsc() / tsc_per_us;
}

static inline long get_ms()
{
  struct timeval now;
  gettimeofday(&now, NULL);
  return now.tv_sec * 1000 + now.tv_usec / 1000;
}

static inline uint64_t get_nanos(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
}

/*****************************************************************************/

/*****************************************************************************/
/********************************** Metrics **********************************/

static void tp_metrics_save(struct loadgen *lg, uint64_t now)
{
  int i, j;

  for (i = 0; i < lg->conf->ncores; i++)
  {
    for (j = 0; j < lg->conf->nconns; j++)
    {
      lg->tpmets[lg->i_mets].nreqs += lg->cores[i].conns[j].nreqs;
    }
  }

  fprintf(stderr, "NREQS=%ld I=%d\n", lg->tpmets[lg->i_mets].nreqs, lg->i_mets);
  lg->tpmets[lg->i_mets].ts = now;
  lg->i_mets++;
}

static void latency_add(struct loadgen *lg, uint64_t lat)
{
  __sync_fetch_and_add(&lg->lat_hist[lat], 1);
}

static void latency_percentiles(struct loadgen *lg)
{
  int i, j;
  uint64_t total_count, cumu_count;
  uint64_t targets[HIST_PERCENTILES];

  total_count = 0;
  for (int i = 0; i < HIST_BUCKETS; i++)
  {
    total_count += lg->lat_hist[i];
  }

  // Find target count for percentile
  for (int i = 0; i < HIST_PERCENTILES; i++)
  {
    targets[i] = total_count * lg->latmets.percentiles[i];
  }

  // Find bins corresponding to target counts
  for (i = 0; i < HIST_PERCENTILES; i++)
  {
    cumu_count = 0;
    for (j = 0; j < HIST_BUCKETS; j++)
    {
      cumu_count += lg->lat_hist[j];
      if (cumu_count >= targets[i])
      {
        lg->latmets.latencies[i] = j;
        break;
      }
    }
  }
}

static void summarize_metrics(struct loadgen *lg, int i_mets)
{
  int i;
  double avg_tp;

  avg_tp = (double) lg->tpmets[i_mets - 1].nreqs / (double) i_mets;
  fprintf(stderr, "Avg TP: %f reqs/s\n", avg_tp);
  for (i = 0; i < HIST_PERCENTILES; i++)
  {
    fprintf(stderr, "%lfp: %ld\n",
        lg->latmets.percentiles[i], lg->latmets.latencies[i]);
  }
}

/*****************************************************************************/

int main(int argc, char **argv)
{
  int ret;
  uint64_t now, last_save, tsc_per_us;
  struct loadgen lg;
  struct config conf;

  srand(time(NULL));

  // Get calibration for tsc
  tsc_per_us = get_tsc_calibration();
  if (tsc_per_us == 0)
  {
    fprintf(stderr, "main: failed to get tsc calibration\n");
    exit(-1);
  }

  // Get configuration from args
  ret = init_config(&conf);
  if (ret < 0)
  {
    fprintf(stderr, "main: failed to initialize config\n");
    exit(-1);
  }

  ret = parse_args(argc, argv, &conf);
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
  ret = init_loadgen(&lg, &conf, tsc_per_us);
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
  uint64_t end_time = get_us_tsc(tsc_per_us) + lg.conf->duration * 1000000;
  now = get_us_tsc(tsc_per_us);
  last_save = now;
  while (now < end_time)
  {
    if ((now - last_save) >= 1000000)
    {
      tp_metrics_save(&lg, now);
      last_save = now;
    }
    now = get_us_tsc(tsc_per_us);
  }

  // Kill running threads
  stop_cores(&lg);

  // Close all Redis connections
  redis_close_all(&lg);

  // Calculate latency percentiles from histogram
  latency_percentiles(&lg);

  // Print metrics for experiment run
  summarize_metrics(&lg, lg.i_mets);

  return 0;
}
