// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
#define SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "asio.hpp"  // NOLINT(build/include_subdir)
#include "asio/steady_timer.hpp"

#include "tendisplus/server/server_params.h"
#include "tendisplus/utils/atomic_utility.h"
#include "tendisplus/utils/invariant.h"
#include "tendisplus/utils/status.h"
#include "tendisplus/utils/time.h"

namespace tendisplus {

class IOCtxException : public std::exception {
 public:
  IOCtxException() : std::exception() {}
};

class PoolMatrix {
 public:
  PoolMatrix operator-(const PoolMatrix& right);
  Atom<uint64_t> inQueue{0};
  Atom<uint64_t> executing{0};
  Atom<uint64_t> executed{0};
  Atom<uint64_t> queueTime{0};
  Atom<uint64_t> executeTime{0};
  std::string toString() const;
  std::string getInfoString() const;
  void reset();
};
class TimerWrapper{
public:
    explicit TimerWrapper(asio::io_context& ctx) : _timer(ctx) {}

    template <typename Time>
    void expires_at(Time time) { _timer.expires_at(time); }

    template <typename Rep, typename Period>
    void expires_after(const std::chrono::duration<Rep, Period>& duration) {
        _timer.expires_after(duration);
    }

    template <typename Handler>
    void async_wait(Handler&& handler) { 
        _timer.async_wait(std::forward<Handler>(handler)); 
    }

    void cancel() { _timer.cancel(); }

private:
    asio::steady_timer _timer;
};
// TODO(pecochen): currently only support static thread-num
// It's better to adaptively resize thread-pool by pressure
class WorkerPool {
 public:
  explicit WorkerPool(const std::string& name,
                      std::shared_ptr<PoolMatrix> poolMatrix);
  WorkerPool(const WorkerPool&) = delete;
  WorkerPool(WorkerPool&&) = delete;
  Status startup(size_t poolSize, bool simpleName = false);
  bool isFull() const;
  template <typename fn>
  void schedule(fn&& task) {
    int64_t enQueueTs = nsSinceEpoch();
    ++_matrix->inQueue;
    auto taskWrap = [this, mytask = std::move(task), enQueueTs]() mutable {
      int64_t outQueueTs = nsSinceEpoch();
      _matrix->queueTime += outQueueTs - enQueueTs;
      ++_matrix->executing;
      try {
        mytask();
      } catch (const IOCtxException& ex) {
        LOG(INFO) << "catch IOCtxException," << _name << ":"
                  << getCurThreadId();
        throw ex;
      } catch (const std::exception& ex) {
        LOG(ERROR) << "schedule task error:" << ex.what();
        INVARIANT_D(0);
      }
      --_matrix->inQueue;
      --_matrix->executing;
      int64_t endExeTs = nsSinceEpoch();
      _matrix->executeTime += endExeTs - outQueueTs;
      ++_matrix->executed;
    };
    asio::post(*_ioCtx, std::move(taskWrap));
    // NOTE(deyukong): use asio::post rather than ctx.post, the latter one
    // only support copyable callbacks. which means you cannot use a lambda
    // which captures unique_Ptr as params
    // refers here: https://github.com/boostorg/asio/issues/61
    // _ioCtx->post(std::move(taskWrap));
  }
  void stop();
  size_t size() const;
  void resize(size_t poolSize);
  std::string getName() const {
    return _name;
  }
  uint64_t timer_add(std::function<void()> cb, 
                    const std::chrono::microseconds& timeout) {
    return schedule_timer_impl(std::move(cb), timeout, false);
  }

    void timer_cancel(uint64_t timer_id) {
        asio::post(*_ioCtx, [this, timer_id] {
            cancel_timer(timer_id);
        });
    }

    // 添加周期定时器
    uint64_t add_periodic_timer(std::function<void()> cb, 
                                const std::chrono::microseconds& interval) 
    {
        return schedule_timer_impl(std::move(cb), interval, true);
    }
 private:
  void consumeTasks(size_t idx);
  void resizeIncrease(size_t size);
  void resizeDecrease(size_t size);
  mutable std::mutex _mutex;
  std::atomic<bool> _isRunning;
  std::unique_ptr<asio::io_context> _ioCtx;
  const std::string _name;
  std::shared_ptr<PoolMatrix> _matrix;
  std::atomic<uint64_t> _idGenerator;
  std::map<std::thread::id, std::thread> _threads;
  uint64_t schedule_timer_impl(std::function<void()> cb, 
                                std::chrono::microseconds timeout, 
                                bool periodic) {
    if (!_isRunning) return 0;

    const uint64_t timer_id = ++_idGenerator;
    
    // 使用 std::function 解决 lambda 递归引用问题
    auto timer_handler = std::make_shared<std::function<void(const asio::error_code&)>>();
    
    // 跨线程操作需要保护
    // asio::steady_timer timer(*_ioCtx);
    // timer.expires_at(asio::steady_timer::clock_type::now() + timeout);
    asio::post(*_ioCtx, [this, timer_id, cb = std::move(cb), timeout, periodic, timer_handler] {
        // 创建定时器对象
        auto timer = std::make_shared<TimerWrapper>(*_ioCtx);
        timer->expires_after(timeout);
        
        // 保存到活跃定时器表
        {
            std::lock_guard<std::mutex> lock(_timersMutex);
            _activeTimers[timer_id] = timer;
        }
        
        // 定义可递归调用的处理函数
        *timer_handler = [this, timer_id, cb, periodic, timeout, timer_handler]
                         (const asio::error_code& ec) mutable 
        {
            // 处理被取消的定时器
            if (ec == asio::error::operation_aborted || !_isRunning.load()) 
                return;
            
            // 重新调度周期定时器
            if (periodic) {
                auto new_timer = std::make_shared<TimerWrapper>(*_ioCtx);
                new_timer->expires_after(timeout);
                
                {
                    std::lock_guard<std::mutex> lock(_timersMutex);
                    _activeTimers[timer_id] = new_timer;
                }
                
                // 再次绑定自身为处理函数
                new_timer->async_wait(*timer_handler);
            } else {
                // 单次定时器完成后移除
                std::lock_guard<std::mutex> lock(_timersMutex);
                _activeTimers.erase(timer_id);
            }
            
            // 提交实际任务到线程池
            schedule([cb = std::move(cb)] {
                cb();
            });
        };
        
        // 开始等待
        timer->async_wait(*timer_handler);
    });
    
    return timer_id;
}

    // 取消定时器
    void cancel_timer(uint64_t timer_id) {
        std::lock_guard<std::mutex> lock(_timersMutex);
        auto it = _activeTimers.find(timer_id);
        if (it != _activeTimers.end()) {
            it->second->cancel();
            _activeTimers.erase(it);
        }
    }

    // 清理所有定时器
    void clear_all_timers() {
        std::lock_guard<std::mutex> lock(_timersMutex);
        for (auto& [id, timer] : _activeTimers) {
            timer->cancel();
        }
        _activeTimers.clear();
    }
    // 新增定时器管理成员
    mutable std::mutex _timersMutex;
    std::unordered_map<uint64_t, std::shared_ptr<TimerWrapper>> _activeTimers;
};

}  // namespace tendisplus
#endif  // SRC_TENDISPLUS_NETWORK_WORKER_POOL_H_
