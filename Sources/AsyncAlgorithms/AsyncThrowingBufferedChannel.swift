//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Async Algorithms open source project
//
// Copyright (c) 2021 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

final class AsyncThrowingBufferedChannel<Element: Sendable, Failure: Error>: AsyncSequence, Sendable {
  public typealias Element = Element
  public typealias AsyncIterator = Iterator

  enum Termination: Sendable {
    case finished
    case failure(Failure)
  }

  struct Awaiting: Hashable {
    let id: Int
    let continuation: UnsafeContinuation<Element?, Error>?

    static func placeHolder(id: Int) -> Awaiting {
      Awaiting(id: id, continuation: nil)
    }

    func hash(into hasher: inout Hasher) {
      hasher.combine(self.id)
    }

    static func == (lhs: Awaiting, rhs: Awaiting) -> Bool {
      lhs.id == rhs.id
    }
  }

  enum SendDecision {
    case resume(Awaiting, Element)
    case finish([Awaiting])
    case fail([Awaiting], Error)
    case nothing
  }

  enum AwaitingDecision {
    case resume(Element?)
    case fail(Error)
    case suspend
  }

  enum Value {
    case element(Element)
    case termination(Termination)
  }

  enum State {
    case active(queue: [Value], awaitings: Set<Awaiting>)
    case terminated(Termination)

    static var initial: State {
      .active(queue: [], awaitings: [])
    }
  }

  let ids: ManagedCriticalState<Int>
  let state: ManagedCriticalState<State>

  init() {
    self.ids = ManagedCriticalState(0)
    self.state = ManagedCriticalState(.initial)
  }

  func generateId() -> Int {
    self.ids.withCriticalRegion { ids in
      ids += 1
      return ids
    }
  }

  func send(_ value: Value) {
    let decision = self.state.withCriticalRegion { state -> SendDecision in
      switch state {
        case var .active(queue, awaitings):
          if !awaitings.isEmpty {
            switch value {
              case .element(let element):
                let awaiting = awaitings.removeFirst()
                state = .active(queue: queue, awaitings: awaitings)
                return .resume(awaiting, element)
              case .termination(.finished):
                state = .terminated(.finished)
                return .finish(Array(awaitings))
              case .termination(.failure(let error)):
                state = .terminated(.failure(error))
                return .fail(Array(awaitings), error)
            }
          } else {
            switch value {
              case .termination(.finished) where queue.isEmpty:
                state = .terminated(.finished)
              case .termination(.failure(let error)) where queue.isEmpty:
                state = .terminated(.failure(error))
              case .element, .termination:
                queue.append(value)
                state = .active(queue: queue, awaitings: awaitings)
            }
            return .nothing
          }
        case .terminated:
          return .nothing
      }
    }

    switch decision {
      case .nothing:
        break
      case .finish(let awaitings):
        awaitings.forEach { $0.continuation?.resume(returning: nil) }
      case .fail(let awaitings, let error):
        awaitings.forEach { $0.continuation?.resume(throwing: error) }
      case let .resume(awaiting, element):
        awaiting.continuation?.resume(returning: element)
    }
  }

  func send(_ element: Element) {
    self.send(.element(element))
  }

  func fail(_ error: Failure) where Failure == Error {
    self.send(.termination(.failure(error)))
  }

  func finish() {
    self.send(.termination(.finished))
  }

  func next(onSuspend: (() -> Void)? = nil) async throws -> Element? {
    let awaitingId = self.generateId()
    let cancellation = ManagedCriticalState<Bool>(false)

    return try await withTaskCancellationHandler { [state] in
      let awaiting = state.withCriticalRegion { state -> Awaiting? in
        cancellation.withCriticalRegion { cancellation in
          cancellation = true
        }
        switch state {
          case .active(let queue, var awaitings):
            let awaiting = awaitings.remove(.placeHolder(id: awaitingId))
            state = .active(queue: queue, awaitings: awaitings)
            return awaiting
          case .terminated:
            return nil
        }
      }

      awaiting?.continuation?.resume(returning: nil)
    } operation: {
      try await withUnsafeThrowingContinuation { [state] (continuation: UnsafeContinuation<Element?, Error>) in
        let decision = state.withCriticalRegion { state -> AwaitingDecision in
          let isCancelled = cancellation.withCriticalRegion { $0 }
          guard !isCancelled else { return .resume(nil) }

          switch state {
            case var .active(queue, awaitings):
              if !queue.isEmpty {
                let value = queue.removeFirst()
                switch value {
                  case .termination(.finished):
                    state = .terminated(.finished)
                    return .resume(nil)
                  case .termination(.failure(let error)):
                    state = .terminated(.failure(error))
                    return .fail(error)
                  case .element(let element):
                    state = .active(queue: queue, awaitings: awaitings)
                    return .resume(element)
                }
              } else {
                awaitings.update(with: Awaiting(id: awaitingId, continuation: continuation))
                state = .active(queue: queue, awaitings: awaitings)
                return .suspend
              }
            case .terminated(.finished):
              return .resume(nil)
            case .terminated(.failure(let error)):
              return .fail(error)
          }
        }

        switch decision {
          case .resume(let element): continuation.resume(returning: element)
          case .fail(let error): continuation.resume(throwing: error)
          case .suspend:
            onSuspend?()
        }
      }
    }
  }

  public func makeAsyncIterator() -> AsyncIterator {
    Iterator(
      channel: self
    )
  }

  public struct Iterator: AsyncIteratorProtocol {
    let channel: AsyncThrowingBufferedChannel<Element, Failure>

    public func next() async throws -> Element? {
      try await self.channel.next()
    }
  }
}
