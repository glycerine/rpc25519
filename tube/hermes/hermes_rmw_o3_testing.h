#define _HAS_ITERATOR_DEBUGGING 0
struct ivy_gen {virtual int choose(int rng,const char *name) = 0;};
#include "z3++.h"
#include "ivy_hash.hpp"
typedef std::string __strlit;
extern std::ofstream __ivy_out;
void __ivy_exit(int);

template <typename D, typename R>
struct thunk {
    virtual R operator()(const D &) = 0;
    int ___ivy_choose(int rng,const char *name,int id) {
        return 0;
    }
};
template <typename D, typename R, class HashFun = hash_space::hash<D> >
struct hash_thunk {
    thunk<D,R> *fun;
    hash_space::hash_map<D,R,HashFun> memo;
    hash_thunk() : fun(0) {}
    hash_thunk(thunk<D,R> *fun) : fun(fun) {}
    ~hash_thunk() {
//        if (fun)
//            delete fun;
    }
    R &operator[](const D& arg){
        std::pair<typename hash_space::hash_map<D,R>::iterator,bool> foo = memo.insert(std::pair<D,R>(arg,R()));
        R &res = foo.first->second;
        if (foo.second && fun)
            res = (*fun)(arg);
        return res;
    }
};


    class reader;
    class timer;

class hermes_rmw_o3_testing {
  public:
    typedef hermes_rmw_o3_testing ivy_class;

    std::vector<std::string> __argv;
#ifdef _WIN32
    void *mutex;  // forward reference to HANDLE
#else
    pthread_mutex_t mutex;
#endif
    void __lock();
    void __unlock();

#ifdef _WIN32
    std::vector<HANDLE> thread_ids;

#else
    std::vector<pthread_t> thread_ids;

#endif
    void install_reader(reader *);
    void install_thread(reader *);
    void install_timer(timer *);
    virtual ~hermes_rmw_o3_testing();
    std::vector<int> ___ivy_stack;
    ivy_gen *___ivy_gen;
    int ___ivy_choose(int rng,const char *name,int id);
    virtual void ivy_assert(bool,const char *){}
    virtual void ivy_assume(bool,const char *){}
    virtual void ivy_check_progress(int,int){}
    enum ltask{ready_finish,o3_finish};
    enum hermes_protocol__hstate{hermes_protocol__hs_valid,hermes_protocol__hs_invalid,hermes_protocol__hs_invalid_write,hermes_protocol__hs_write,hermes_protocol__hs_replay};
struct __tup__unsigned__unsigned_long_long {
    unsigned arg0;
    unsigned long long arg1;
__tup__unsigned__unsigned_long_long(){}__tup__unsigned__unsigned_long_long(const unsigned &arg0,const unsigned long long &arg1) : arg0(arg0),arg1(arg1){}
        size_t __hash() const { size_t hv = 0;
hv += hash_space::hash<unsigned>()(arg0);
hv += hash_space::hash<unsigned long long>()(arg1);
return hv;
}
};

class hash____tup__unsigned__unsigned_long_long {
    public:
        size_t operator()(const hermes_rmw_o3_testing::__tup__unsigned__unsigned_long_long &__s) const {
            return hash_space::hash<unsigned>()(__s.arg0)+hash_space::hash<unsigned long long>()(__s.arg1);
        }
    };
    bool hermes_protocol__le[16][16];
    ltask hermes_protocol__ready_task;
    ltask hermes_protocol__o3_task;
    unsigned hermes_protocol__init_ts;
    unsigned hermes_protocol__init_value;
    unsigned hermes_protocol__init_epoch;
    hermes_protocol__hstate hermes_protocol__state[4];
    unsigned hermes_protocol__cur_ts[4];
    unsigned hermes_protocol__cur_value[4];
    bool hermes_protocol__cur_rmw[4];
    unsigned hermes_protocol__last_writer[4];
    bool hermes_protocol__writer_live[4];
    bool hermes_protocol__live[4];
    bool hermes_protocol__pending[4];
    unsigned hermes_protocol__pending_ts[4];
    bool hermes_protocol__pending_rmw[4];
    bool hermes_protocol__acked[4][4];
    bool hermes_protocol__ready[4];
    unsigned hermes_protocol__ready_epoch[4];
    bool hermes_protocol__seen_epoch[16];
    bool hermes_protocol__epoch_done[16];
    bool hermes_protocol__ready_epoch_active[16];
    bool hermes_protocol__client_write_pending[4];
    unsigned hermes_protocol__client_write_value[4];
    bool hermes_protocol__client_rmw_pending[4];
    unsigned hermes_protocol__client_rmw_value[4];
    bool hermes_protocol__seen_ts[16];
    bool hermes_protocol__parent[16][16];
    bool hermes_protocol__same_parent[16][16];
    unsigned hermes_protocol__parent_ts[16];
    hash_thunk<__tup__unsigned__unsigned_long_long,bool> hermes_protocol__ts_version;
    bool hermes_protocol__same_version[16][16];
    bool hermes_protocol__next_version[16][16];
    bool hermes_protocol__ts_value[16][16];
    bool hermes_protocol__ts_rmw[16];
    bool hermes_protocol__rmw_conflict[16];
    bool hermes_protocol__write_conflict[16];
    bool hermes_protocol__inv_write[4][16][16];
    bool hermes_protocol__inv_rmw[4][16][16];
    bool hermes_protocol__ack_msg[4][4][16];
    bool hermes_protocol__val_msg[16];
    bool hermes_protocol__o3_quorum[4][4][16];
    bool hermes_protocol__complete_try[4];
    bool hermes_protocol__o3_try[4][4][16];
    bool hermes_protocol__completed[16];
    bool _generating;
    long long __CARD__hermes_protocol__version;
    long long __CARD__hermes_protocol__node;
    long long __CARD__hermes_protocol__epoch;
    long long __CARD__hermes_protocol__ts;
    long long __CARD__hermes_protocol__value;
    virtual bool hermes_protocol__lt(unsigned X, unsigned Y);
    virtual bool hermes_protocol__local_key_known(unsigned N);
    virtual bool hermes_protocol__write_then_write_order_ok(unsigned N, unsigned M);
    virtual bool hermes_protocol__rmw_then_write_order_ok(unsigned N, unsigned M);
    virtual bool hermes_protocol__rmw_then_rmw_order_ok(unsigned N, unsigned M);
    virtual bool hermes_protocol__can_start_client_write(unsigned N, unsigned T);
    virtual bool hermes_protocol__can_start_client_rmw(unsigned N, unsigned T);
    hermes_rmw_o3_testing();
void __init();
    virtual unsigned long long ext__hermes_protocol__version__next(unsigned long long x);
    virtual void ext__hermes_protocol__local_write(unsigned n, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__local_rmw(unsigned n, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__receive_write_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__receive_rmw_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__receive_ack(unsigned n, unsigned a, unsigned t);
    virtual void ext__hermes_protocol__complete_current(unsigned n);
    virtual void ext__hermes_protocol__complete_overwritten(unsigned n);
    virtual void ext__hermes_protocol__complete_ready(unsigned n);
    virtual void ext__hermes_protocol__o3_observe_quorum(unsigned n, unsigned c, unsigned t);
    virtual void ext__hermes_protocol__o3_complete(unsigned n, unsigned c, unsigned t);
    virtual void ext__hermes_protocol__mark_ready(unsigned n, unsigned e);
    virtual void ext__hermes_protocol__receive_validate(unsigned n, unsigned t);
    virtual void ext__hermes_protocol__replay_after_failure(unsigned n);
    virtual void ext__hermes_protocol__fail(unsigned n);
    virtual void ext__hermes_protocol__ambient__idle();
    virtual void ext__hermes_protocol__ambient__start_write(unsigned n, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__start_rmw(unsigned n, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__client_write_arrive(unsigned n, unsigned v);
    virtual void ext__hermes_protocol__ambient__client_rmw_arrive(unsigned n, unsigned v);
    virtual void ext__hermes_protocol__ambient__drain_client_write(unsigned n, unsigned t);
    virtual void ext__hermes_protocol__ambient__drain_client_rmw(unsigned n, unsigned t);
    virtual void ext__hermes_protocol__ambient__deliver_write_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__duplicate_write_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__deliver_rmw_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__duplicate_rmw_inv(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__collect_ack(unsigned n, unsigned a, unsigned t);
    virtual void ext__hermes_protocol__ambient__make_ready(unsigned n, unsigned e);
    virtual void ext__hermes_protocol__ambient__finish_current(unsigned n);
    virtual void ext__hermes_protocol__ambient__finish_overwritten(unsigned n);
    virtual void ext__hermes_protocol__ambient__complete_ready_driver(unsigned n);
    virtual void ext__hermes_protocol__ambient__observe_o3(unsigned n, unsigned c, unsigned t);
    virtual void ext__hermes_protocol__ambient__complete_o3(unsigned n, unsigned c, unsigned t);
    virtual void ext__hermes_protocol__ambient__validate_some(unsigned n, unsigned t);
    virtual void ext__hermes_protocol__ambient__replay_dead_writer(unsigned n);
    virtual void ext__hermes_protocol__ambient__overwrite_pending_write(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__abort_pending_rmw_by_write(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__stale_rmw_pushback(unsigned n, unsigned s, unsigned t, unsigned v);
    virtual void ext__hermes_protocol__ambient__fail_pending_node(unsigned n);
    virtual void ext__hermes_protocol__ambient__fail_observed_writer(unsigned n);
    virtual void ext__hermes_protocol__ambient__finish_conflicted_ready(unsigned n);
    virtual void ext__hermes_protocol__ambient__fail_nonlast(unsigned n);
    virtual void ext__hermes_protocol__ambient__scenario_write_quorum();
    virtual void ext__hermes_protocol__ambient__scenario_rmw_quorum();
    virtual void ext__hermes_protocol__ambient__scenario_o3_write();
    virtual void ext__hermes_protocol__ambient__scenario_overwritten_write();
    virtual void ext__hermes_protocol__ambient__scenario_rmw_aborted_by_write();
    virtual void ext__hermes_protocol__ambient__scenario_rmw_race_pushback();
    virtual void ext__hermes_protocol__ambient__scenario_writer_fail_replay();
                                                                                                                                                                                                                                        void __tick(int timeout);
};
