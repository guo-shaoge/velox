/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/Operator.h"
#include <iostream>
// #define BOOST_STACKTRACE_USE_ADDR2LINE
// #include <boost/stacktrace.hpp>
// #include <iostream>

namespace facebook::velox::exec::test {

exec::BlockingReason TaskQueue::enqueue(
    RowVectorPtr vector,
    velox::ContinueFuture* future) {
    // gjt todo: del this, just for debug
    // std::cout << boost::stacktrace::stacktrace() << std::endl;
    std::cout << "InVelox log enqueue called begin" << std::endl;
  if (!vector) {
    std::cout << "InVelox log enqueue called no vector" << std::endl;
    std::lock_guard<std::mutex> l(mutex_);
    ++producersFinished_;
    if (consumerBlocked_) {
      consumerBlocked_ = false;
      consumerPromise_.setValue();
    }
    return exec::BlockingReason::kNotBlocked;
  }

  auto bytes = vector->retainedSize();
  TaskQueueEntry entry{std::move(vector), bytes};

  std::lock_guard<std::mutex> l(mutex_);
  // Check inside 'mutex_'
  if (closed_) {
    throw std::runtime_error("Consumer cursor is closed");
  }
  queue_.push_back(std::move(entry));
  totalBytes_ += bytes;
  if (consumerBlocked_) {
    consumerBlocked_ = false;
    consumerPromise_.setValue();
  }
  if (totalBytes_ > maxBytes_) {
    auto [unblockPromise, unblockFuture] = makeVeloxContinuePromiseContract();
    producerUnblockPromises_.emplace_back(std::move(unblockPromise));
    *future = std::move(unblockFuture);
    return exec::BlockingReason::kWaitForConsumer;
  }
  return exec::BlockingReason::kNotBlocked;
}

RowVectorPtr TaskQueue::dequeue() {
  for (;;) {
    RowVectorPtr vector;
    std::vector<ContinuePromise> mayContinue;
    {
      std::lock_guard<std::mutex> l(mutex_);
      std::cout << "InVelox log TaskQueue::dequeue begin" << std::endl;
      if (!queue_.empty()) {
        std::cout << "InVelox log TaskQueue::dequeue queue_ not empty" << std::endl;
        auto result = std::move(queue_.front());
        queue_.pop_front();
        totalBytes_ -= result.bytes;
        vector = std::move(result.vector);
        if (totalBytes_ < maxBytes_ / 2) {
          mayContinue = std::move(producerUnblockPromises_);
        }
      } else if (
          std::cout << "InVelox log TaskQueue::dequeue queue_ queue_ empty" << \
          numProducers_.has_value() << "; " << producersFinished_ << std::endl;
          numProducers_.has_value() && producersFinished_ == numProducers_) {
        return nullptr;
      }
      if (!vector) {
          std::cout << "InVelox log TaskQueue::dequeue vector is null" << std::endl;
        consumerBlocked_ = true;
        consumerPromise_ = ContinuePromise();
        consumerFuture_ = consumerPromise_.getFuture();
      }
    }
    // outside of 'mutex_'
    for (auto& promise : mayContinue) {
      promise.setValue();
    }
    if (vector) {
      return vector;
    }
    consumerFuture_.wait();
  }
}

void TaskQueue::close() {
  std::lock_guard<std::mutex> l(mutex_);
  closed_ = true;
  for (auto& promise : producerUnblockPromises_) {
    promise.setValue();
  }
  producerUnblockPromises_.clear();
}

bool TaskQueue::hasNext() {
  std::lock_guard<std::mutex> l(mutex_);
  return !queue_.empty();
}

std::atomic<int32_t> TaskCursor::serial_;

TaskCursor::TaskCursor(const CursorParameters& params)
    : maxDrivers_{params.maxDrivers},
      numConcurrentSplitGroups_{params.numConcurrentSplitGroups},
      numSplitGroups_{params.numSplitGroups},
      params_(params) {
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::cout << "InVelox log start to build TaskCursor" << std::endl;
  if (params.queryCtx) {
    queryCtx = params.queryCtx;
  } else {
    queryCtx = core::QueryCtx::createForTest();
  }

  queue_ = std::make_shared<TaskQueue>(params.bufferedBytes);
  // Captured as a shared_ptr by the consumer callback of task_.
  auto queue = queue_;
  core::PlanFragment planFragment{
      params.planNode, params.executionStrategy, params.numSplitGroups};
  task_ = std::make_shared<exec::Task>(
      fmt::format("test_cursor {}", ++serial_),
      std::move(planFragment),
      params.destination,
      std::move(queryCtx),
      // consumer
      [queue](RowVectorPtr vector, velox::ContinueFuture* future) {
        std::cout << "InVelox log queue consumer called" << std::endl;
        if (!vector) {
          return queue->enqueue(nullptr, future);
        }
        // Make sure to load lazy vector if not loaded already.
        for (auto& child : vector->children()) {
          child->loadedVector();
        }
        RowVectorPtr copy = std::dynamic_pointer_cast<RowVector>(
            BaseVector::create(vector->type(), vector->size(), queue->pool()));
        copy->copy(vector.get(), 0, 0, vector->size());
        return queue->enqueue(std::move(copy), future);
      });
  std::cout << "InVelox log done to build TaskCursor" << std::endl;
}

void TaskCursor::start() {
  if (!started_) {
    started_ = true;
    std::cout << "InVelox log TaskCursor::start() begin" << std::endl;
    exec::Task::start(task_, maxDrivers_, numConcurrentSplitGroups_);
    queue_->setNumProducers(numSplitGroups_ * task_->numOutputDrivers());
    std::cout << "InVelox log TaskCursor::start() done" << numSplitGroups_ << " ; " <<  task_->numOutputDrivers() << std::endl;
  }
}

bool TaskCursor::moveNext() {
  std::cout << "InVelox log TaskCursor::moveNext() begin" << std::endl;
  start();
  current_ = queue_->dequeue();
  if (task_->error()) {
    std::rethrow_exception(task_->error());
  }
  if (!current_) {
    std::cout << "InVelox log TaskCursor::moveNext() no current_" << std::endl;
    atEnd_ = true;
  }
 std::cout << "InVelox log TaskCursor::moveNext() done" << std::endl;
  return current_ != nullptr;
}

bool TaskCursor::hasNext() {
  return queue_->hasNext();
}

bool RowCursor::next() {
  if (++currentRow_ < numRows_) {
    return true;
  }
  if (!cursor_->moveNext()) {
    return false;
  }
  auto vector = cursor_->current();
  numRows_ = vector->size();
  if (!numRows_) {
    return next();
  }
  currentRow_ = 0;
  if (decoded_.empty()) {
    decoded_.resize(vector->childrenSize());
    for (int32_t i = 0; i < vector->childrenSize(); ++i) {
      decoded_[i] = std::make_unique<DecodedVector>();
    }
  }
  allRows_.resize(vector->size());
  allRows_.setAll();
  for (int32_t i = 0; i < decoded_.size(); ++i) {
    decoded_[i]->decode(*vector->childAt(i), allRows_);
  }
  return true;
}

bool RowCursor::hasNext() {
  return currentRow_ < numRows_ || cursor_->hasNext();
}

} // namespace facebook::velox::exec::test
