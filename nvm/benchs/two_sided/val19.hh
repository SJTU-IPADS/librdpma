/*!
  The lscpu setup of val19.
 */
namespace nvm {

static const int per_socket_cores = 12; // TODO!! hard coded
// const int per_socket_cores = 8;//reserve 2 cores

static int socket_0[] = {0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22,
                         24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46};

static int socket_1[] = {1,  3,  5,  7,  9,  11, 13, 15, 17, 19, 21, 23,
                         25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47};

int bind_to_core(int t_id) {

  if (t_id >= (per_socket_cores * 2))
    return 0;

  int x = t_id;
  int y = 0;

#ifdef SCALE
  assert(false);
  // specific  binding for scale tests
  int mac_per_node = 16 / nthreads; // there are total 16 threads avialable
  int mac_num = current_partition % mac_per_node;

  if (mac_num < mac_per_node / 2) {
    y = socket_0[x + mac_num * nthreads];
  } else {
    y = socket_one[x + (mac_num - mac_per_node / 2) * nthreads];
  }
#else
  // bind ,andway
  if (x >= per_socket_cores) {
    // there is no other cores in the first socket
    y = socket_0[x - per_socket_cores];
  } else {
    y = socket_1[x];
  }

#endif

  // fprintf(stdout,"worker: %d binding %d\n",x,y);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}
}
