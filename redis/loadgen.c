#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <math.h>

#define MAX_KEY_BUF 64
#define MAX_COMMAND_BUF 1024
#define MIN_KEY 1
#define MAX_KEY 10000
#define DEFAULT_VAL_SIZE 64
#define DEFAULT_SET_RATIO 1
#define DEFAULT_GET_RATIO 9

enum dist {UNIFORM, ZIPFIAN, SEQUENTIAL};

struct loadgen 
{
  int conn_fd;
  int duration;
  struct timeval start_time;
  
  int set_ratio;
  int get_ratio;
  
  enum dist distribution;
  int sequential_counter;

  int *set_keys;
  int set_key_count;
};

void print_usage(const char *prog_name) 
{
  printf("Usage: %s [options]                                              \n"
    "General options:                                                      \n"
    "  -h <ADDR>          Redis server ip address                          \n"
    "  -p <INT>           Redis server listening port                      \n"
    "  -t <INT>           Number of seconds to run                         \n"
    "                                                                      \n"
    "Key options:                                                          \n"
    "  -r <SET:GET>       Ratio of SET and GET commands [default: %d:%d]   \n" 
    "  -d <distribution>  Distribution to generate keys [default: uniform] \n"
    "    Options: uniform, zipfian, sequential                             \n",
    prog_name, DEFAULT_SET_RATIO, DEFAULT_GET_RATIO);
}

long get_time_ms() 
{
  struct timeval now;
  gettimeofday(&now, NULL);
  return now.tv_sec * 1000 + now.tv_usec / 1000;
}

int sample_uniform(int min, int max) 
{
  return rand() % (max - min + 1) + min;
}

int sample_zipf(int s, int n) 
{
  double p = (rand() / (RAND_MAX + 1.0));
  double sum_prob = 0.0;
  
  for (int i = 1; i <= n; i++) 
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
  switch (lg->distribution) 
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

void generate_load(struct loadgen *lg) 
{
  char key_str[MAX_KEY_BUF];
  const char set_val[DEFAULT_VAL_SIZE];
  
  int total_requests = lg->set_ratio + lg->get_ratio;
  long end_time = get_time_ms() + lg->duration * 1000;

  while (get_time_ms() < end_time) 
  {
    for (int i = 0; i < total_requests; i++) {
      int key = generate_key(lg);
      int key_len = snprintf(key_str, MAX_KEY_BUF, "%d", key);
      
      if (i < lg->set_ratio) 
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

int connect_to_redis(const char *ip, int port) 
{
  struct sockaddr_in server_addr;
  int conn_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (conn_fd < 0) 
  {
    perror("Socket creation failed");
    exit(EXIT_FAILURE);
  }

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip);

  if (connect(conn_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) 
  {
    perror("Connection failed");
    exit(EXIT_FAILURE);
  }

  return conn_fd;
}

int main(int argc, char **argv) 
{
  const char *redis_ip = NULL;
  int redis_port = 0;
  int set_ratio = DEFAULT_SET_RATIO;
  int get_ratio = DEFAULT_GET_RATIO;
  enum dist distribution = UNIFORM;
  int duration = 0;

  // Parse command-line arguments
  int opt;
  while ((opt = getopt(argc, argv, "h:p:r:d:t:")) != -1) 
  {
    switch (opt) 
    {
        case 'h':
          redis_ip = optarg;
          break;
        case 'p':
          redis_port = atoi(optarg);
          break;
        case 'r': {
          char *token = strtok(optarg, ":");
          
          if (!token) 
          {
            print_usage(argv[0]);
            return EXIT_FAILURE;
          }
          
          set_ratio = atoi(token);
          token = strtok(NULL, ":");
          
          if (!token) 
          {
            print_usage(argv[0]);
            return EXIT_FAILURE;
          }
          
          get_ratio = atoi(token);
          break;
        }
        case 'd':
          if (strcmp(optarg, "uniform") == 0) 
          {
            distribution = UNIFORM;
          } 
          else if (strcmp(optarg, "zipfian") == 0) 
          {
            distribution = ZIPFIAN;
          } 
          else if (strcmp(optarg, "sequential") == 0) 
          {

            distribution = SEQUENTIAL;
          } 
          else 
          {
            print_usage(argv[0]);
            return EXIT_FAILURE;
          }
          break;
        case 't':
          duration = atoi(optarg);
          break;
        default:
          print_usage(argv[0]);
          return EXIT_FAILURE;
    }
  }

  if (!redis_ip || redis_port == 0 || duration == 0) 
  {
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }

  srand(time(NULL));
  
  struct loadgen lg;
  lg.set_ratio = set_ratio;
  lg.get_ratio = get_ratio;
  lg.distribution = distribution;
  lg.duration = duration;
  lg.sequential_counter = MIN_KEY;
  lg.set_key_count = 0;
  lg.set_keys = (int *)malloc(MAX_KEY * sizeof(int));
  lg.conn_fd = connect_to_redis(redis_ip, redis_port);
  gettimeofday(&lg.start_time, NULL);
   
  generate_load(&lg);

  free(lg.set_keys);
  close(lg.conn_fd);
  printf("Load generation completed.\n");

  return EXIT_SUCCESS;
}

