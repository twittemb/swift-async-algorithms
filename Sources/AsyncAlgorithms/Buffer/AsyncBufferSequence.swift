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

@rethrows
public protocol BufferStorage<Element>: Sendable {
  associatedtype Element
  func send(element: Element) async
  func fail(error: Error)
  func finish()
  func next() async -> Result<Element?, Error>
}

extension AsyncSequence where Self: Sendable {
  public func buffer(
    limit: UInt
  ) -> AsyncBufferSequence<Self, SuspendingBufferStorage<Element>> {
    self.buffer(storage: SuspendingBufferStorage(limit: limit))
  }

  public func buffer(
    policy: QueuedBufferPolicy
  ) -> AsyncBufferSequence<Self, QueuedBufferStorage<Element>> {
    self.buffer(storage: QueuedBufferStorage(policy: policy))
  }

  public func buffer<Storage: BufferStorage>(
    storage: Storage
  ) -> AsyncBufferSequence<Self, Storage> where Self.Element == Storage.Element {
    AsyncBufferSequence(base: self, storage: storage)
  }
}

public struct AsyncBufferSequence<Base: AsyncSequence & Sendable, Storage: BufferStorage>: AsyncSequence, Sendable
where Base.Element == Storage.Element {
  public typealias Element = Base.Element
  public typealias AsyncIterator = Iterator

  let base: Base
  let storage: Storage

  public init(
    base: Base,
    storage: Storage
  ) {
    self.base = base
    self.storage = storage
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
    let storage: Storage

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
