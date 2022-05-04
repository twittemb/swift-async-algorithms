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

public extension AsyncSequence where Self: Sendable, Self.AsyncIterator: Sendable, Self.Element: Sendable {
  func split() -> (AsyncSplit2Sequence<Self>, AsyncSplit2Sequence<Self>) {
    let baseStateMachine = Split2StateMachine(base: self)
    return (
      AsyncSplit2Sequence(baseStateMachine: baseStateMachine, child: .first),
      AsyncSplit2Sequence(baseStateMachine: baseStateMachine, child: .second)
    )
  }
}

final class Split2StateMachine<Base: AsyncSequence>: Sendable where Base: Sendable, Base.AsyncIterator: Sendable, Base.Element: Sendable {
  let first: AsyncChannel<Result<Base.Element?, Error>>
  let second: AsyncChannel<Result<Base.Element?, Error>>

  enum Child {
    case first
    case second
  }

  struct ChildrenState {
    var firsts = Set<Int>()
    var seconds = Set<Int>()
    var generatedIds = 0
  }

  enum BaseState {
    case idle(iterator: Base.AsyncIterator)
    case busy(task: Task<(), Never>)
  }

  let baseState: ManagedCriticalState<BaseState>
  let childrenState: ManagedCriticalState<ChildrenState>

  init(base: Base) {
    self.first = AsyncChannel<Result<Base.Element?, Error>>()
    self.second = AsyncChannel<Result<Base.Element?, Error>>()
    self.baseState = ManagedCriticalState(.idle(iterator: base.makeAsyncIterator()))
    self.childrenState = ManagedCriticalState(ChildrenState())
  }

  func makeAsyncIterator(for child: Child) -> (AsyncChannel<Result<Base.Element?, Error>>.AsyncIterator, Int) {
    return self.childrenState.withCriticalRegion { childrenState -> (AsyncChannel<Result<Base.Element?, Error>>.AsyncIterator, Int) in
      childrenState.generatedIds += 1
      switch child {
      case .first:
        childrenState.firsts.update(with: childrenState.generatedIds)
        return (self.first.makeAsyncIterator(), childrenState.generatedIds)
      case .second:
        childrenState.seconds.update(with: childrenState.generatedIds)
        return (self.second.makeAsyncIterator(), childrenState.generatedIds)
      }
    }
  }

  func next(for child: Child, id: Int, iterator: inout AsyncChannel<Result<Base.Element?, Error>>.AsyncIterator) async -> Result<Base.Element?, Error>? {
    guard !Task.isCancelled else {
      self.cancel(for: child, id: id)
      return nil
    }

    return await withTaskCancellationHandler {
      self.cancel(for: child, id: id)
    } operation: {
      let task = self.requestNextFromBase()
      let element = await iterator.next()
      await task.value
      return element
    }
  }

  func cancel(for child: Child, id: Int) {
    switch child {
    case .first:
      let shouldCancelSend = self.childrenState.withCriticalRegion { childrenState -> Bool in
        childrenState.firsts.remove(id)
        return childrenState.firsts.isEmpty
      }
      if shouldCancelSend {
        self.first.finish()
      }
    case .second:
      let shouldCancelSend = self.childrenState.withCriticalRegion { childrenState -> Bool in
        childrenState.seconds.remove(id)
        return childrenState.seconds.isEmpty
      }
      if shouldCancelSend {
        self.second.finish()
      }
    }
  }

  func requestNextFromBase() -> Task<Void, Never> {
    return self.baseState.withCriticalRegion { baseState -> Task<Void, Never> in
      switch baseState {
      case let .idle(iterator):
        let task = Task {
          var mutableLocalIterator = iterator
          do {
            let next = try await mutableLocalIterator.next()
            await withTaskGroup(of: Void.self) { group in
              group.addTask {
                await self.first.send(.success(next))
              }

              group.addTask {
                await self.second.send(.success(next))
              }
            }

            self.baseState.withCriticalRegion { baseState in
              baseState = .idle(iterator: mutableLocalIterator)
            }
          } catch {
            await withTaskGroup(of: Void.self) { group in
              group.addTask {
                await self.first.send(.failure(error))
              }

              group.addTask {
                await self.second.send(.failure(error))
              }
            }
          }
        }
        baseState = .busy(task: task)
        return task
      case let .busy(task):
        return task
      }
    }
  }
}

public struct AsyncSplit2Sequence<Base: AsyncSequence>: AsyncSequence, Sendable
where Base: Sendable,
      Base.AsyncIterator: Sendable,
      Base.Element: Sendable {
  public typealias Element = Base.Element
  public typealias AsyncIterator = Iterator

  let baseStateMachine: Split2StateMachine<Base>
  let child: Split2StateMachine<Base>.Child

  init(baseStateMachine: Split2StateMachine<Base>, child: Split2StateMachine<Base>.Child) {
    self.baseStateMachine = baseStateMachine
    self.child = child
  }

  public func makeAsyncIterator() -> Iterator {
    Iterator(baseStateMachine: self.baseStateMachine, child: self.child)
  }

  public struct Iterator: AsyncIteratorProtocol {
    let baseStateMachine: Split2StateMachine<Base>
    let child: Split2StateMachine<Base>.Child
    var childIterator: AsyncChannel<Result<Base.Element?, Error>>.AsyncIterator
    var isTerminated = false
    let id: Int

    init(baseStateMachine: Split2StateMachine<Base>, child: Split2StateMachine<Base>.Child) {
      self.baseStateMachine = baseStateMachine
      self.child = child
      (self.childIterator, self.id) = self.baseStateMachine.makeAsyncIterator(for: self.child)
    }

    public mutating func next() async rethrows -> Element? {
      guard !self.isTerminated else { return nil }
      
      let element = await self.baseStateMachine.next(for: self.child, id: self.id, iterator: &self.childIterator)
      if element == nil {
        isTerminated = true
      }

      return try element?._rethrowGet()
    }
  }
}