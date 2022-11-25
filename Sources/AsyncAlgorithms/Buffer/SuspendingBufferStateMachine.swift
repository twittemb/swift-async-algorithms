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

struct SuspendingBufferStateMachine<Element> {
  private typealias SuspendedUpstream = (continuation: UnsafeContinuation<Void, Never>, element: Element)

  private enum State {
    case idle
    case buffering(buffer: Deque<Element>)
    case waitingForDownstream(suspendedUpstream: SuspendedUpstream, buffer: Deque<Element>)
    case waitingForUpstream(downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>)
    case finished(buffer: Deque<Element>?, error: (Error)?)
    case modifying
  }

  private var state: State
  private let limit: UInt

  init(limit: UInt) {
    self.state = .idle
    self.limit = limit
  }

  enum NewElementAction {
    case suspend
    case resumeUpstream
    case resumeUpstreamAndDownstream(
      downstreamContinuation: UnsafeContinuation<Result<Element?, Error>, Never>,
      result: Result<Element?, Error>
    )
  }

  mutating func newElement(
    continuation: UnsafeContinuation<Void, Never>,
    element: Element
  ) -> NewElementAction {
    switch self.state {
      case .idle:
        var buffer = Deque<Element>(minimumCapacity: Int(self.limit))
        buffer.append(element)
        self.state = .buffering(buffer: buffer)
        return .resumeUpstream

      case .buffering(var buffer):
        if buffer.count < self.limit {
          // there are available slots in the buffer
          self.state = .modifying
          buffer.append(element)
          self.state = .buffering(buffer: buffer)
          return .resumeUpstream
        } else {
          // the buffer is full, the suspension is confirmed, we need the downstream to consume an element
          self.state = .waitingForDownstream(
            suspendedUpstream: (continuation: continuation, element: element),
            buffer: buffer
          )
          return .suspend
        }

      case .waitingForDownstream:
        preconditionFailure("Invalid state. The upstream is already suspended.")

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .idle
        return .resumeUpstreamAndDownstream(
          downstreamContinuation: downstreamContinuation,
          result: .success(element)
        )

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        return .resumeUpstream
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

      case .waitingForDownstream:
        preconditionFailure("Invalid state. The upstream is suspended.")

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
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

      case .waitingForDownstream:
        preconditionFailure("Invalid state. The upstream is suspended.")

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
    case resumeUpstream(upstreamContinuation: UnsafeContinuation<Void, Never>)
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

      case .waitingForDownstream(let suspendedUpstream, let buffer):
        self.state = .finished(buffer: buffer, error: nil)
        return .resumeUpstream(upstreamContinuation: suspendedUpstream.continuation)

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        return .none
    }
  }

  enum NewIterationAction {
    case suspend
    case resumeDownstream(result: Result<Element?, Error>)
    case resumeUpstreamAndDownstream(upstreamContinuation: UnsafeContinuation<Void, Never>, result: Result<Element?, Error>)
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

      case .waitingForDownstream(let suspendedUpstream, var buffer):
        precondition(!buffer.isEmpty, "Invalid state. The buffer should not be empty.")
        self.state = .modifying
        let element = buffer.popFirst()!
        buffer.append(suspendedUpstream.element)
        self.state = .buffering(buffer: buffer)
        return .resumeUpstreamAndDownstream(upstreamContinuation: suspendedUpstream.continuation, result: .success(element))

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
    case resumeUpstream(upstreamContinuation: UnsafeContinuation<Void, Never>)
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

      case .waitingForDownstream(let suspendedUpstream, _):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeUpstream(upstreamContinuation: suspendedUpstream.continuation)

      case .waitingForUpstream(let downstreamContinuation):
        self.state = .finished(buffer: nil, error: nil)
        return .resumeDownstreamWithNil(downstreamContinuation: downstreamContinuation)

      case .modifying:
        preconditionFailure("Invalid state.")

      case .finished:
        return .none
    }
  }
}
