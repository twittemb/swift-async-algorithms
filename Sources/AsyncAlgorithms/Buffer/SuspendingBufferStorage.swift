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
struct SuspendingBufferStorage<Element>: Sendable {
  private let stateMachine: ManagedCriticalState<SuspendingBufferStateMachine<Element>>

  init(limit: UInt) {
    self.stateMachine = ManagedCriticalState(SuspendingBufferStateMachine(limit: limit))
  }

  func send(element: Element) async {
    let isCancelled = ManagedCriticalState(false)

    await withTaskCancellationHandler {
      await withUnsafeContinuation { (continuation: UnsafeContinuation<Void, Never>) in
        self.stateMachine.withCriticalRegion { stateMachine in
          let isCancelled = isCancelled.withCriticalRegion { $0 }
          guard !isCancelled else { return }

          let action = stateMachine.newElement(continuation: continuation, element: element)

          switch action {
            case .suspend:
              break
            case .resumeUpstream:
              continuation.resume()
            case .resumeUpstreamAndDownstream(let downstreamContinuation, let result):
              continuation.resume()
              downstreamContinuation.resume(returning: result)
          }
        }
      }
    } onCancel: {
      self.stateMachine.withCriticalRegion { stateMachine in
        isCancelled.withCriticalRegion { $0 = true }
        let action = stateMachine.upstreamWasCancelled()

        switch action {
          case .none:
            break
          case .resumeUpstream(let upstreamContinuation):
            upstreamContinuation.resume()
          case .resumeDownstreamWithNil(let downstreamContinuation):
            downstreamContinuation.resume(returning: .success(nil))
        }
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

  func next() async -> Result<Element?, Error> {
    guard !Task.isCancelled else { return .success(nil) }

    let isCancelled = ManagedCriticalState(false)

    return await withTaskCancellationHandler {
      return await withUnsafeContinuation { (continuation: UnsafeContinuation<Result<Element?, any Error>, Never>) in
        self.stateMachine.withCriticalRegion { stateMachine in
          let isCancelled = isCancelled.withCriticalRegion { $0 }
          guard !isCancelled else {
            continuation.resume(returning: .success(nil))
            return
          }

          let action = stateMachine.newIteration(downstreamContinuation: continuation)

          switch action {
            case .suspend:
              break
            case .resumeDownstream(let result):
              continuation.resume(returning: result)
            case .resumeUpstreamAndDownstream(let upstreamContinuation, let result):
              upstreamContinuation.resume()
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
          case .resumeUpstream(let upstreamContinuation):
            upstreamContinuation.resume()
          case .resumeDownstreamWithNil(let downstreamContinuation):
            downstreamContinuation.resume(returning: .success(nil))
        }
      }
    }
  }
}
