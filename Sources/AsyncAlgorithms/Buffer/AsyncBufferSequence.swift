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

public protocol BufferStorage<Element> {
  associatedtype Element
  @Sendable func send(element: Element) async
  @Sendable func fail(error: Error)
  @Sendable func finish()
  @Sendable func next() async -> Result<Element?, Error>
}

extension AsyncSequence where Self: Sendable {
  public func buffer(
    limit: UInt
  ) -> AsyncBufferSequence<Self> {
    self.buffer(storage: SuspendingBufferStorage(limit: limit))
  }

  public func buffer(
    policy: QueuedBufferPolicy
  ) -> AsyncBufferSequence<Self> {
    self.buffer(storage: QueuedBufferStorage(policy: policy))
  }

  public func buffer<Storage: BufferStorage>(
    storage: Storage
  ) -> AsyncBufferSequence<Self> where Self.Element == Storage.Element {
    AsyncBufferSequence(base: self, storage: storage)
  }
}

public struct AsyncBufferSequence<Base: AsyncSequence & Sendable>: AsyncSequence, Sendable {
  public typealias Element = Base.Element
  public typealias AsyncIterator = Iterator

  let base: Base
  let send: @Sendable (Element) async -> Void
  let fail: @Sendable (Error) -> Void
  let finish: @Sendable () -> Void
  let next: @Sendable () async -> Result<Element?, Error>

  public init(
    base: Base,
    storage: some BufferStorage<Element>
  ) {
    self.base = base
    self.send = storage.send(element:)
    self.fail = storage.fail(error:)
    self.finish = storage.finish
    self.next = storage.next
  }

  public func makeAsyncIterator() -> Iterator {
    return Iterator(
      base: self.base,
      send: self.send,
      fail: self.fail,
      finish: self.finish,
      next: self.next
    )
  }

  public struct Iterator: AsyncIteratorProtocol {
    var task: Task<Void, Never>? = nil
    var taskIsSpawned = false

    let base: Base
    let send: @Sendable (Element) async -> Void
    let fail: @Sendable (Error) -> Void
    let finish: @Sendable () -> Void
    let next: @Sendable () async -> Result<Element?, Error>

    init(
      base: Base,
      send: @Sendable @escaping (Element) async -> Void,
      fail: @Sendable @escaping (Error) -> Void,
      finish: @Sendable @escaping () -> Void,
      next: @Sendable @escaping () async -> Result<Element?, Error>
    ) {
      self.base = base
      self.send = send
      self.fail = fail
      self.finish = finish
      self.next = next
    }

    public mutating func next() async rethrows -> Element? {
      try await withTaskCancellationHandler {
        if !taskIsSpawned {
          self.taskIsSpawned = true
          self.task = Task { [base, send, finish, fail] in
            var iterator = base.makeAsyncIterator()
            do {
              while let element = try await iterator.next() {
                await send(element)
              }
              finish()
            } catch {
              fail(error)
            }
          }
        }
        return try await self.next()._rethrowGet()
      } onCancel: { [task] in
        task?.cancel()
      }
    }
  }
}
