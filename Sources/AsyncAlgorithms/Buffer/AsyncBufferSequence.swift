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

//public protocol BufferStorage<Element> {
//  associatedtype Element
//  @Sendable func send(element: Element) async
//  @Sendable func fail(error: Error)
//  @Sendable func finish()
//  @Sendable func next() async -> Result<Element?, Error>
//}

extension AsyncSequence where Self: Sendable {
  public func buffer(
    limit: UInt
  ) -> AsyncSuspendingBufferSequence<Self> {
    AsyncSuspendingBufferSequence(base: self, limit: limit)
  }

  public func buffer(
    policy: QueuedBufferPolicy
  ) -> AsyncQueuedBufferSequence<Self> {
    AsyncQueuedBufferSequence(base: self, policy: policy)
  }
}

public struct AsyncSuspendingBufferSequence<Base: AsyncSequence & Sendable>: AsyncSequence, Sendable {
  public typealias Element = Base.Element
  public typealias AsyncIterator = Iterator

  let base: Base
  let storage: SuspendingBufferStorage<Element>

  public init(
    base: Base,
    limit: UInt
  ) {
    self.base = base
    self.storage = SuspendingBufferStorage(limit: limit)
  }

  public func makeAsyncIterator() -> Iterator {
    return Iterator(
      base: self.base,
      storage: self.storage
    )
  }

  public struct Iterator: AsyncIteratorProtocol {
    var task: Task<Void, Never>? = nil
    var taskIsSpawned = false

    let base: Base
    let storage: SuspendingBufferStorage<Element>

    public mutating func next() async rethrows -> Element? {
      try await withTaskCancellationHandler {
        if !taskIsSpawned {
          self.taskIsSpawned = true
          self.task = Task { [base, storage] in
            var iterator = base.makeAsyncIterator()
            do {
              while let element = try await iterator.next() {
                await storage.send(element: element)
              }
              storage.finish()
            } catch {
              storage.fail(error: error)
            }
          }
        }
        return try await self.storage.next()._rethrowGet()
      } onCancel: { [task] in
        task?.cancel()
      }
    }
  }
}

public struct AsyncQueuedBufferSequence<Base: AsyncSequence & Sendable>: AsyncSequence, Sendable {
  public typealias Element = Base.Element
  public typealias AsyncIterator = Iterator

  let base: Base
  let storage: QueuedBufferStorage<Element>

  public init(
    base: Base,
    policy: QueuedBufferPolicy
  ) {
    self.base = base
    self.storage = QueuedBufferStorage(policy: policy)
  }

  public func makeAsyncIterator() -> Iterator {
    return Iterator(
      base: self.base,
      storage: self.storage
    )
  }

  public struct Iterator: AsyncIteratorProtocol {
    var task: Task<Void, Never>? = nil
    var taskIsSpawned = false

    let base: Base
    let storage: QueuedBufferStorage<Element>

    public mutating func next() async rethrows -> Element? {
      try await withTaskCancellationHandler {
        if !taskIsSpawned {
          self.taskIsSpawned = true
          self.task = Task { [base, storage] in
            var iterator = base.makeAsyncIterator()
            do {
              while let element = try await iterator.next() {
                await storage.send(element: element)
              }
              storage.finish()
            } catch {
              storage.fail(error: error)
            }
          }
        }
        return try await self.storage.next()._rethrowGet()
      } onCancel: { [task] in
        task?.cancel()
      }
    }
  }
}
