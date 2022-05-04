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

@preconcurrency import XCTest
import AsyncAlgorithms

final class TestSplit: XCTestCase {
  func test_split_makes_symmetrical_sequences() async {
    var a = GatedSequence([1, 2, 3])
    let finished = expectation(description: "finished")
    finished.expectedFulfillmentCount = 2

    let (seq1, seq2) = a.split()

    let validator1 = Validator<Int>()
    validator1.test(seq1) { iterator in
      let pastEnd = await iterator.next()
      XCTAssertNil(pastEnd)
      finished.fulfill()
    }

    let validator2 = Validator<Int>()
    validator2.test(seq2) { iterator in
      let pastEnd = await iterator.next()
      XCTAssertNil(pastEnd)
      finished.fulfill()
    }

    var value1 = await validator1.validate()
    XCTAssertEqual(value1, [])

    var value2 = await validator2.validate()
    XCTAssertEqual(value2, [])

    a.advance()
    value1 = await validator1.validate()
    XCTAssertEqual(value1, [1])
    value2 = await validator2.validate()
    XCTAssertEqual(value2, [1])

    a.advance()
    value1 = await validator1.validate()
    XCTAssertEqual(value1, [1, 2])
    value2 = await validator2.validate()
    XCTAssertEqual(value2, [1, 2])

    a.advance()
    value1 = await validator1.validate()
    XCTAssertEqual(value1, [1, 2, 3])
    value2 = await validator2.validate()
    XCTAssertEqual(value2, [1, 2, 3])

    wait(for: [finished], timeout: 1.0)
    value1 = validator1.current
    value2 = validator2.current
    XCTAssertEqual(value1, [1, 2, 3])
    XCTAssertEqual(value2, [1, 2, 3])
  }

  func test_split_executes_upstream_next_function_once_per_iteration_when_two_children() async {
    let finished = expectation(description: "finished")
    finished.expectedFulfillmentCount = 2

    let expected = (0...9).map { _ in 1 }
    let expectedEvents: [ReportingAsyncSequence<Int>.Event] = [.makeAsyncIterator] + (0...10).map { _ in .next }

    let sequence = ReportingAsyncSequence(expected)
    let (seq1, seq2) = sequence.split()

    Task {
      var collected = [Int]()
      for await element in seq1 {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      finished.fulfill()
    }

    Task {
      var collected = [Int]()
      for await element in seq2 {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      finished.fulfill()
    }

    wait(for: [finished], timeout: 1)
    XCTAssertEqual(sequence.events, expectedEvents)
  }

  func test_split_makes_children_throws_when_upstream_sequence_throws() async {
    let failed = expectation(description: "failed")
    failed.expectedFulfillmentCount = 2

    let sequence = [1, 2, 3].async.map { try throwOn(2, $0) }
    let (seq1, seq2) = sequence.split()
    let expected = [1]

    Task {
      var collected = [Int]()
      do {
        for try await element in seq1 {
          collected.append(element)
        }
        XCTFail("The sequence1 should fail")
      } catch {
        XCTAssertEqual(collected, expected)
        failed.fulfill()
      }
    }

    Task {
      var collected = [Int]()
      do {
        for try await element in seq2 {
          collected.append(element)
        }
        XCTFail("The sequence2 should fail")
      } catch {
        XCTAssertEqual(collected, expected)
        failed.fulfill()
      }
    }

    wait(for: [failed], timeout: 1)
  }

  func test_split_cancels_first_channel_when_first_child_is_cancelled() async {
    let expTask1HasDoneSomeIterations = expectation(description: "")
    let expTasksHaveFinished = expectation(description: "")
    expTasksHaveFinished.expectedFulfillmentCount = 2

    let expected = (0...49).map { $0 }

    let sequence = expected.async
    let (a, b) = sequence.split()

    let task1 = Task {
      var collected = [Int]()
      for await element in a {
        collected.append(element)
        if element == 9 {
          expTask1HasDoneSomeIterations.fulfill()
        }
      }
      XCTAssertTrue(collected.starts(with: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
      XCTAssertTrue(collected.count < 50)
      expTasksHaveFinished.fulfill()
    }

    Task {
      var collected = [Int]()
      for await element in b {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      expTasksHaveFinished.fulfill()
    }

    wait(for: [expTask1HasDoneSomeIterations], timeout: 1)

    task1.cancel()

    wait(for: [expTasksHaveFinished], timeout: 1)
  }

  func test_split_cancels_second_channel_when_second_child_is_cancelled() async {
    let expTask2HasDoneSomeIterations = expectation(description: "")
    let expTasksHaveFinished = expectation(description: "")
    expTasksHaveFinished.expectedFulfillmentCount = 2

    let expected = (0...49).map { $0 }

    let sequence = expected.async
    let (a, b) = sequence.split()

    Task {
      var collected = [Int]()
      for await element in a {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      expTasksHaveFinished.fulfill()
    }

    let task2 = Task {
      var collected = [Int]()
      for await element in b {
        collected.append(element)
        if element == 9 {
          expTask2HasDoneSomeIterations.fulfill()
        }
      }
      XCTAssertTrue(collected.starts(with: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
      XCTAssertTrue(collected.count < 50)
      expTasksHaveFinished.fulfill()
    }

    wait(for: [expTask2HasDoneSomeIterations], timeout: 1)

    task2.cancel()

    wait(for: [expTasksHaveFinished], timeout: 1)
  }

  func test_split_continue_first_channel_when_one_of_first_children_is_cancelled() async {
    let expConsumer1HasDoneSomeIterations = expectation(description: "")
    let expConsumer1BisHasDoneSomeIterations = expectation(description: "")

    let expTasksHaveFinished = expectation(description: "")
    expTasksHaveFinished.expectedFulfillmentCount = 3

    let expected = (0...49).map { $0 }

    let sequence = expected.async
    let (a, b) = sequence.split()

    let task1 = Task {
      var collected = [Int]()
      var elementHasBeenReceived = false
      for await element in a {
        collected.append(element)
        if !elementHasBeenReceived {
          elementHasBeenReceived = true
          expConsumer1HasDoneSomeIterations.fulfill()
        }
      }
      expTasksHaveFinished.fulfill()
    }

    Task {
      var elementHasBeenReceived = false
      for await _ in a {
        if !elementHasBeenReceived {
          elementHasBeenReceived = true
          expConsumer1BisHasDoneSomeIterations.fulfill()
        }
      }
      expTasksHaveFinished.fulfill()
    }

    Task {
      var collected = [Int]()
      for await element in b {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      expTasksHaveFinished.fulfill()
    }

    wait(for: [expConsumer1HasDoneSomeIterations, expConsumer1BisHasDoneSomeIterations], timeout: 10)

    task1.cancel()

    wait(for: [expTasksHaveFinished], timeout: 10)
  }

  func test_split_continue_second_channel_when_one_of_second_children_is_cancelled() async {
    let expConsumer2HasDoneSomeIterations = expectation(description: "")
    let expConsumer2BisHasDoneSomeIterations = expectation(description: "")

    let expTasksHaveFinished = expectation(description: "")
    expTasksHaveFinished.expectedFulfillmentCount = 3

    let expected = (0...49).map { $0 }

    let sequence = expected.async
    let (a, b) = sequence.split()

    Task {
      var collected = [Int]()
      for await element in a {
        collected.append(element)
      }
      XCTAssertEqual(collected, expected)
      expTasksHaveFinished.fulfill()
    }

    let task2 = Task {
      var collected = [Int]()
      var elementHasBeenReceived = false
      for await element in b {
        collected.append(element)
        if !elementHasBeenReceived {
          elementHasBeenReceived = true
          expConsumer2HasDoneSomeIterations.fulfill()
        }
      }
      expTasksHaveFinished.fulfill()
    }

    Task {
      var elementHasBeenReceived = false
      for await _ in b {
        if !elementHasBeenReceived {
          elementHasBeenReceived = true
          expConsumer2BisHasDoneSomeIterations.fulfill()
        }
      }
      expTasksHaveFinished.fulfill()
    }



    wait(for: [expConsumer2HasDoneSomeIterations, expConsumer2BisHasDoneSomeIterations], timeout: 1)

    task2.cancel()

    wait(for: [expTasksHaveFinished], timeout: 1)
  }
}