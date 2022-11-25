//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Async Algorithms open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//
@_implementationOnly import DequeModule

struct QueuedBufferStateMachine<Element> {
  private enum State {
    case idle
    case buffering(buffer: Deque<Element>)
    case waitingForUpstream(downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>)
    case finished(buffer: Deque<Element>?, error: (Error)?)
    case modifying
  }

  private var state: State
  private let policy: QueuedBufferPolicy

  init(policy: QueuedBufferPolicy) {
    precondition(policy.positiveLimit, "The limit should be positive.")
    self.state = .idle
    self.policy = policy
  }

  enum NewElementAction {
    case none
    case resumeDownstream(
      downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>,
      result: Result<Element?, Error>
    )
  }

  mutating func newElement(element: Element) -> NewElementAction {
    switch (self.state, self.policy) {
      case (.idle, _):
        var buffer = Deque<Element>()
        buffer.append(element)
        self.state = .buffering(buffer: buffer)
        return .none

      case (.buffering(var buffer), .unbounded):
        self.state = .modifying
        buffer.append(element)
        self.state = .buffering(buffer: buffer)
        return .none

      case (.buffering(var buffer), .bufferingNewest(let limit)):
        self.state = .modifying
        if buffer.count >= limit {
          _ = buffer.popFirst()
        }
        buffer.append(element)
        self.state = .buffering(buffer: buffer)
        return .none

      case (.buffering(var buffer), .bufferingOldest(let limit)):
        self.state = .modifying
        if buffer.count < limit {
          buffer.append(element)
        }
        self.state = .buffering(buffer: buffer)
        return .none

      case (.waitingForUpstream(let downstreamContinuation), _):
        self.state = .idle
        return .resumeDownstream(
          downstreamContinuation: downstreamContinuation,
          result: .success(element)
        )

      case (.modifying, _):
        preconditionFailure("Invalid state.")

      case (.finished, _):
        return .none
    }
  }

  enum FinishAction {
    case none
    case resumeDownstreamWithNil(downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>)
  }

  mutating func finish() -> FinishAction {
    switch self.state {
      case .idle:
        self.state = .finished(buffer: nil, error: nil)
        return .none

      case .buffering(let buffer):
        self.state = .finished(buffer: buffer, error: nil)
        return .none

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        self.state = .finished(buffer: nil, error: nil)
        return .none
    }
  }

  enum FailAction {
    case none
    case resumeDownstream(
      downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>,
      result: Result<Element?, Error>
    )
  }

  mutating func fail(error: Error) -> FailAction {
    switch self.state {
      case .idle:
        self.state = .finished(buffer: nil, error: error)
        return .none

      case .buffering(let buffer):
        self.state = .finished(buffer: buffer, error: error)
        return .none

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: error)
        return .resumeDownstream(downstreamContinuation: downstreamContinuation, result: .failure(error))

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        preconditionFailure("Invalid state. The upstream is already finished.")
    }
  }

  enum UpstreamWasCancelledAction {
    case none
    case resumeDownstreamWithNil(downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>)
  }

  mutating func upstreamWasCancelled() -> UpstreamWasCancelledAction {
    switch self.state {
      case .idle:
        self.state = .finished(buffer: nil, error: nil)
        return .none

      case .buffering:
        self.state = .finished(buffer: nil, error: nil)
        return .none

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        self.state = .finished(buffer: nil, error: nil)
        return .none
    }
  }

  enum NewIterationAction {
    case suspend
    case resumeDownstream(result: Result<Element?, Error>)
  }

  mutating func newIteration(
    downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>
  ) -> NewIterationAction {
    switch self.state {
      case .idle:
        self.state = .waitingForUpstream(downstreamContinuation: downstreamContinuation)
        return .suspend

      case .buffering(var buffer):
        precondition(!buffer.isEmpty, "Invalid state. The buffer should not be empty.")
        self.state = .modifying
        let element = buffer.popFirst()!
        if buffer.isEmpty {
          self.state = .idle
        } else {
          self.state = .buffering(buffer: buffer)
        }
        return .resumeDownstream(result: .success(element))

      case .waitingForUpstream:
        preconditionFailure("Invalid state. The downstream is already suspended.")

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished(.some(var buffer), let error):
        precondition(!buffer.isEmpty, "Invalid state. The buffer should not be empty.")
        self.state = .modifying
        let element = buffer.popFirst()!
        if buffer.isEmpty {
          self.state = .finished(buffer: nil, error: error)
        } else {
          self.state = .finished(buffer: buffer, error: error)
        }
        return .resumeDownstream(result: .success(element))

      case .finished(.none, .some(let error)):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstream(result: .failure(error))

      case .finished(.none, .none):
        return .resumeDownstream(result: .success(nil))
    }
  }

  enum DownstreamWasCancelledAction {
    case none
    case resumeDownstreamWithNil(downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>)
  }

  mutating func downstreamWasCancelled() -> DownstreamWasCancelledAction {
    switch self.state {
      case .idle:
        self.state = .finished(buffer: nil, error: nil)
        return .none

      case .buffering:
        self.state = .finished(buffer: nil, error: nil)
        return .none

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        self.state = .finished(buffer: nil, error: nil)
        return .none
    }
  }
}
