// Raw random read-modify-write floor: N independent updates into a large table.
// No hashing, no probing logic - just what memory allows. Independent addresses, so
// the core's own out-of-order window supplies the memory-level parallelism.
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
static inline uint64_t mix(uint64_t x){x^=x>>33;x*=0xff51afd7ed558ccdULL;x^=x>>33;x*=0xc4ceb9fe1a85ec53ULL;x^=x>>33;return x;}
int main(int argc,char**argv){
  size_t bytes=(size_t)atof(argv[1])*1e9, n=(size_t)atof(argv[2]); int threads=atoi(argv[3]);
  size_t slots=bytes/8; std::vector<uint64_t> t(slots,0); // touch: zero-filled
  auto work=[&](size_t lo,size_t hi){ for(size_t i=lo;i<hi;i++) t[mix(i)%slots]+=1; };
  auto t0=std::chrono::steady_clock::now();
  std::vector<std::thread> th; size_t per=n/threads;
  for(int k=0;k<threads;k++) th.emplace_back(work,k*per,(k+1)*per);
  for(auto&x:th) x.join();
  double s=std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
  printf("table %.1fGB  rows %.0fM  threads %2d  wall %7.1fms  %.2f ns/row/thread-cpu  %.2f ns/row wall\n",
    bytes/1e9,n/1e6,threads,s*1e3,s*threads/(double)n*1e9,s/(double)n*1e9);
}
