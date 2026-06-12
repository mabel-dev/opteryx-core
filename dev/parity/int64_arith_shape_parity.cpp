#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/int64_arithmetic.h"
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <cstring>
using namespace draken::ops;

// Build vectors of each shape over a logical value sequence `logical` (length n).
static DrakenVector dense_vec(const std::vector<int64_t>& logical, std::vector<int64_t>& store, std::vector<uint32_t>& sel) {
    uint32_t n = logical.size();
    store = logical;
    sel.resize(n); for (uint32_t i=0;i<n;++i) sel[i]=i;
    DrakenVector v; v.data=store.data(); v.selection=sel.data(); v.data_length=n; v.length=n;
    v.validity=nullptr; v.type=DRAKEN_INT64; v.flags=DRAKEN_SEL_IDENTITY|DRAKEN_SEL_PERMUTATION; return v;
}
static DrakenVector const_vec(int64_t val, uint32_t n, std::vector<int64_t>& store, std::vector<uint32_t>& sel) {
    store={val}; sel.assign(n,0);
    DrakenVector v; v.data=store.data(); v.selection=sel.data(); v.data_length=1; v.length=n;
    v.validity=nullptr; v.type=DRAKEN_INT64; v.flags=0; return v;
}
// dict: unique values `uniq`, codes per row
static DrakenVector dict_vec(const std::vector<int64_t>& uniq, const std::vector<uint32_t>& codes, std::vector<int64_t>& store, std::vector<uint32_t>& sel) {
    store=uniq; sel=codes;
    DrakenVector v; v.data=store.data(); v.selection=sel.data(); v.data_length=uniq.size(); v.length=codes.size();
    v.validity=nullptr; v.type=DRAKEN_INT64; v.flags=0; return v;
}
static int64_t logical_at(const DrakenVector& v, uint32_t i){ return ((const int64_t*)v.data)[v.selection[i]]; }
static int64_t res_at(const VecResult& r, uint32_t i){ return ((const int64_t*)r.data)[r.selection[i]]; }

static int fails=0;
static void check(const char* name, const VecResult& r, const DrakenVector& a, const DrakenVector& b, int op) {
    uint32_t n=a.length;
    for (uint32_t i=0;i<n;++i) {
        int64_t x=logical_at(a,i), y=logical_at(b,i), exp;
        switch(op){case 1:exp=x+y;break;case 2:exp=x-y;break;case 3:exp=x*y;break;
                   case 4:exp=(y==0)?0:x/y;break;default:exp=(y==0)?0:x%y;break;}
        if (res_at(r,i)!=exp){ printf("FAIL %s row %u: got %lld exp %lld\n",name,i,(long long)res_at(r,i),(long long)exp); fails++; return; }
    }
    printf("ok %s\n", name);
}

int main(){
    // logical sequences
    std::vector<int64_t> base={7,-3,10,0,5,-8,2,100};
    // dict over base with repeats
    std::vector<int64_t> uniq={7,-3,10,0,5,-8,2,100};
    std::vector<uint32_t> codes={0,1,2,3,4,5,6,7,0,1,2,3};  // 12 rows from 8 uniques (dict)
    std::vector<int64_t> exp_logical; for(auto c:codes) exp_logical.push_back(uniq[c]);

    auto run=[&](int op, const char* opn){
        // dict OP const
        { std::vector<int64_t> sa,sb; std::vector<uint32_t> qa,qb;
          DrakenVector a=dict_vec(uniq,codes,sa,qa); DrakenVector b=const_vec(3,codes.size(),sb,qb);
          VecResult r; switch(op){case 1:r=i64_add(a,b);break;case 2:r=i64_sub(a,b);break;case 3:r=i64_mul(a,b);break;case 4:r=i64_div(a,b);break;default:r=i64_mod(a,b);}
          char nm[64]; snprintf(nm,64,"%s dict,const",opn); check(nm,r,a,b,op);
          // verify shape preserved: dict in → dict out
          if (r.data_length != a.data_length){ printf("FAIL %s shape: data_length %u != %u\n",nm,r.data_length,a.data_length); fails++; }
          draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }
        // const OP dict
        { std::vector<int64_t> sa,sb; std::vector<uint32_t> qa,qb;
          DrakenVector a=const_vec(50,codes.size(),sa,qa); DrakenVector b=dict_vec(uniq,codes,sb,qb);
          VecResult r; switch(op){case 1:r=i64_add(a,b);break;case 2:r=i64_sub(a,b);break;case 3:r=i64_mul(a,b);break;case 4:r=i64_div(a,b);break;default:r=i64_mod(a,b);}
          char nm[64]; snprintf(nm,64,"%s const,dict",opn); check(nm,r,a,b,op);
          if (r.data_length != b.data_length){ printf("FAIL %s shape\n",nm); fails++; }
          draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }
        // dense OP const
        { std::vector<int64_t> sa,sb; std::vector<uint32_t> qa,qb;
          DrakenVector a=dense_vec(base,sa,qa); DrakenVector b=const_vec(4,base.size(),sb,qb);
          VecResult r; switch(op){case 1:r=i64_add(a,b);break;case 2:r=i64_sub(a,b);break;case 3:r=i64_mul(a,b);break;case 4:r=i64_div(a,b);break;default:r=i64_mod(a,b);}
          char nm[64]; snprintf(nm,64,"%s dense,const",opn); check(nm,r,a,b,op);
          draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }
        // const OP const
        { std::vector<int64_t> sa,sb; std::vector<uint32_t> qa,qb;
          DrakenVector a=const_vec(9,5,sa,qa); DrakenVector b=const_vec(2,5,sb,qb);
          VecResult r; switch(op){case 1:r=i64_add(a,b);break;case 2:r=i64_sub(a,b);break;case 3:r=i64_mul(a,b);break;case 4:r=i64_div(a,b);break;default:r=i64_mod(a,b);}
          char nm[64]; snprintf(nm,64,"%s const,const",opn); check(nm,r,a,b,op);
          if (r.data_length != 1){ printf("FAIL %s expected constant result\n",nm); fails++; }
          draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }
        // dense OP dense (no fold — must stay correct)
        { std::vector<int64_t> sa,sb; std::vector<uint32_t> qa,qb;
          DrakenVector a=dense_vec(base,sa,qa); std::vector<int64_t> base2={1,2,3,4,5,6,7,8}; DrakenVector b=dense_vec(base2,sb,qb);
          VecResult r; switch(op){case 1:r=i64_add(a,b);break;case 2:r=i64_sub(a,b);break;case 3:r=i64_mul(a,b);break;case 4:r=i64_div(a,b);break;default:r=i64_mod(a,b);}
          char nm[64]; snprintf(nm,64,"%s dense,dense",opn); check(nm,r,a,b,op);
          draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }
    };
    run(1,"add"); run(2,"sub"); run(3,"mul"); run(4,"div"); run(5,"mod");

    // neg shape preservation (dict)
    { std::vector<int64_t> sa; std::vector<uint32_t> qa; DrakenVector a=dict_vec(uniq,codes,sa,qa);
      VecResult r=i64_neg(a);
      bool ok = r.data_length==a.data_length;
      for(uint32_t i=0;i<a.length;++i) if(res_at(r,i)!=-logical_at(a,i)) ok=false;
      printf("%s neg dict\n", ok?"ok":"FAIL"); if(!ok)fails++;
      draken_free(r.data); if(r.owns_selection) draken_free((void*)r.selection); draken_free(r.validity); }

    printf(fails? "PARITY FAILURES\n":"ALL PARITY OK\n"); return fails?1:0;
}
