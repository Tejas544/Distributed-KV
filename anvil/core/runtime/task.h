// Task<T>: the unit of concurrency in Anvil.
//
// Every protocol in the core is written as coroutines, never threads. Under the
// simulator these are scheduled one at a time on a single OS thread, so
// "concurrency" is a sequence of resumptions the scheduler chooses -- and a
// sequence the scheduler can choose *adversarially*, which is where the bugs
// come from. Under the production runtime the same coroutines are driven by a
// thread-per-core executor.
//
// Lazy start: a Task does nothing until awaited. Eager start would run coroutine
// bodies at construction time, i.e. outside the scheduler's control, and any
// I/O or randomness there would land outside the recorded decision sequence.
//
// Symmetric transfer on completion: final_suspend hands control directly to the
// awaiting coroutine rather than returning to the scheduler loop. That keeps
// deep await chains from growing the stack, and -- more importantly here -- it
// makes resumption order a property of the await graph rather than of the
// scheduler's queue, so it stays identical across platforms.

#ifndef ANVIL_CORE_RUNTIME_TASK_H_
#define ANVIL_CORE_RUNTIME_TASK_H_

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace anvil {

template <typename T>
class Task;

namespace detail {

struct FinalAwaiter {
  bool await_ready() const noexcept { return false; }

  template <typename Promise>
  std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> self) noexcept {
    auto continuation = self.promise().continuation;
    // noop_coroutine when nobody is waiting: the task was spawned rather than
    // awaited, and control returns to the scheduler loop.
    return continuation ? continuation : std::noop_coroutine();
  }

  void await_resume() const noexcept {}
};

template <typename T>
struct TaskPromise {
  std::coroutine_handle<> continuation{};
  std::optional<T> value{};
  std::exception_ptr error{};

  Task<T> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept { return {}; }
  FinalAwaiter final_suspend() noexcept { return {}; }

  template <typename U>
  void return_value(U&& v) {
    value.emplace(std::forward<U>(v));
  }

  void unhandled_exception() noexcept { error = std::current_exception(); }

  T take() {
    if (error) std::rethrow_exception(error);
    return std::move(*value);
  }
};

template <>
struct TaskPromise<void> {
  std::coroutine_handle<> continuation{};
  std::exception_ptr error{};

  Task<void> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept { return {}; }
  FinalAwaiter final_suspend() noexcept { return {}; }

  void return_void() noexcept {}
  void unhandled_exception() noexcept { error = std::current_exception(); }

  void take() {
    if (error) std::rethrow_exception(error);
  }
};

}  // namespace detail

template <typename T = void>
class Task {
 public:
  using promise_type = detail::TaskPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  Task() noexcept = default;
  explicit Task(handle_type h) noexcept : handle_(h) {}

  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task() {
    if (handle_) handle_.destroy();
  }

  bool valid() const noexcept { return static_cast<bool>(handle_); }
  bool done() const noexcept { return !handle_ || handle_.done(); }

  // Ownership transfer for the scheduler, which keeps spawned tasks alive in its
  // own table rather than on someone's stack.
  handle_type release() noexcept { return std::exchange(handle_, {}); }

  class Awaiter {
   public:
    explicit Awaiter(handle_type h) noexcept : handle_(h) {}

    bool await_ready() const noexcept { return !handle_ || handle_.done(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
      handle_.promise().continuation = awaiting;
      return handle_;  // symmetric transfer: start the callee immediately
    }

    T await_resume() { return handle_.promise().take(); }

   private:
    handle_type handle_;
  };

  Awaiter operator co_await() const& noexcept { return Awaiter{handle_}; }
  Awaiter operator co_await() && noexcept { return Awaiter{handle_}; }

 private:
  handle_type handle_{};
};

namespace detail {

template <typename T>
Task<T> TaskPromise<T>::get_return_object() noexcept {
  return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
  return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail

}  // namespace anvil

#endif  // ANVIL_CORE_RUNTIME_TASK_H_
