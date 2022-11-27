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
public enum QueuedBufferPolicy: Sendable {
  case unbounded
  case bufferingNewest(limit: UInt)
  case bufferingOldest(limit: UInt)

  var positiveLimit: Bool {
    switch self {
      case .unbounded:
        return true
      case .bufferingNewest(let limit), .bufferingOldest(let limit):
        return limit > 0
    }
  }
}

struct QueuedBufferStorage<Element>: Sendable {
  private let stateMachine: ManagedCriticalState<QueuedBufferStateMachine<Element>>

  init(policy: QueuedBufferPolicy) {
    self.stateMachine = ManagedCriticalState(QueuedBufferStateMachine(policy: policy))
  }

  func send(element: Element) async {
    self.stateMachine.withCriticalRegion { stateMachine in
      let action = stateMachine.newElement(element: element)

      switch action {
        case .none:
          break
        case .resumeDownstream(let downstreamContinuation, let result):
          downstreamContinuation.resume(returning: result)
      }
    }
  }

  func fail(error: some Error) {
    self.stateMachine.withCriticalRegion { stateMachine in
      let action = stateMachine.fail(error: error)

      switch action {
        case .none:
          break
        case .resumeDownstream(let downstreamContinuation, let result):
          downstreamContinuation.resume(returning: result)
      }
    }
  }

  func finish() {
    self.stateMachine.withCriticalRegion { stateMachine in
      let action = stateMachine.finish()

      switch action {
        case .none:
          break
        case .resumeDownstreamWithNil(let downstreamContinuation):
          downstreamContinuation.resume(returning: .success(nil))
      }
    }
  }

  func next() async -> Result<Element?, Error> {
    guard !Task.isCancelled else {  return .success(nil) }

    let isCancelled = ManagedCriticalState(false)

    return await withTaskCancellationHandler {
      return await withUnsafeContinuation { (continuation: UnsafeContinuation<Result<Element?, any Error>, Never>) in
        self.stateMachine.withCriticalRegion { stateMachine in


          let action = stateMachine.newIteration(downstreamContinuation: continuation)

          switch action {
            case .suspend:
              break
            case .resumeDownstream(let result):
              continuation.resume(returning: result)
          }
        }
      }
    } onCancel: {
      self.stateMachine.withCriticalRegion { stateMachine in
        isCancelled.withCriticalRegion { $0 = true }
        let action = stateMachine.downstreamWasCancelled()

        switch action {
          case .none:
            break
          case .resumeDownstreamWithNil(let downstreamContinuation):
            downstreamContinuation.resume(returning: .success(nil))
        }
      }
    }
  }
}
