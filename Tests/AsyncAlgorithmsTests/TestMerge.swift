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

final class TestMerge2: XCTestCase {
  private func assertSequencesAreMerged(first: [Int], second: [Int]) async {
    let merged = merge(first.async, second.async)
    var collected = [Int]()
    let expected = Set(first + second).sorted()
      
    var iterator = merged.makeAsyncIterator()
    while let item = await iterator.next() {
      collected.append(item)
    }
    let pastEnd = await iterator.next()
      
    XCTAssertNil(pastEnd)
    XCTAssertEqual(Set(collected).sorted(), expected)
  }
  
  private func assertMergedSequenceThrowsWhenFirstThrows(first: [Int], second: [Int], throwOn value: Int) async throws {
    let merged = merge(first.async.map { try throwOn(value, $0) }, second.async)
    var collected = Set<Int>()
    let expected = Set(first.prefix(while: { $0 != value }))

    var iterator = merged.makeAsyncIterator()
    do {
      while let item = try await iterator.next() {
        collected.insert(item)
      }
      XCTFail("Merged sequence should throw after collecting elements from the first sequence up to \(value)")
    } catch {
      XCTAssertEqual(Failure(), error as? Failure)
    }
    let pastEnd = try await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(collected.intersection(expected), expected)
  }
  
  private func assertMergedSequenceThrowsWhenSecondThrows(first: [Int], second: [Int], throwOn value: Int) async throws {
    let merged = merge(first.async, second.async.map { try throwOn(value, $0) })
    var collected = Set<Int>()
    let expected = Set(second.prefix(while: { $0 != value }))

    var iterator = merged.makeAsyncIterator()
    do {
      while let item = try await iterator.next() {
        collected.insert(item)
      }
      XCTFail("Merged sequence should throw after collecting elements from the second sequence up to \(value)")
    } catch {
      XCTAssertEqual(Failure(), error as? Failure)
    }
    let pastEnd = try await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(collected.intersection(expected), expected)
  }
  
  func test_merge_makes_sequence_with_elements_from_sources_when_all_have_same_size() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6]
    )
  }
  
  func test_merge_makes_sequence_with_elements_from_sources_when_first_is_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3, 4, 5, 6, 7],
      second: [8, 9, 10]
    )
  }
  
  func test_merge_makes_sequence_with_elements_from_sources_when_second_is_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6, 7]
    )
  }
  
  func test_merge_produces_three_elements_from_first_and_throws_when_first_is_longer_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenFirstThrows(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8],
      throwOn: 4
    )
  }
  
  func test_merge_produces_three_elements_from_first_and_throws_when_first_is_shorter_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenFirstThrows(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8, 9, 10, 11],
      throwOn: 4
    )
  }
  
  func test_merge_produces_three_elements_from_second_and_throws_when_second_is_longer_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenSecondThrows(
      first: [1, 2, 3],
      second: [4, 5, 6, 7, 8],
      throwOn: 7
    )
  }
  
  func test_merge_produces_three_elements_from_second_and_throws_when_second_is_shorter_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenSecondThrows(
      first: [1, 2, 3, 4, 5, 6, 7],
      second: [7, 8, 9, 10, 11],
      throwOn: 10
    )
  }
  
  func test_merge_makes_sequence_with_ordered_elements_when_sources_follow_a_timeline() {
    validate {
      "a-c-e-g-|"
      "-b-d-f-h"
      merge($0.inputs[0], $0.inputs[1])
      "abcdefgh|"
    }
  }
  
  func test_merge_finishes_when_iteration_task_is_cancelled() async {
    let source1 = Indefinite(value: "test1")
    let source2 = Indefinite(value: "test2")
    let sequence = merge(source1.async, source2.async)
    let finished = expectation(description: "finished")
    let iterated = expectation(description: "iterated")
    let task = Task {
      var firstIteration = false
      for await _ in sequence {
        if !firstIteration {
          firstIteration = true
          iterated.fulfill()
        }
      }
      finished.fulfill()
    }
    // ensure the other task actually starts
    wait(for: [iterated], timeout: 1.0)
    // cancellation should ensure the loop finishes
    // without regards to the remaining underlying sequence
    task.cancel()
    wait(for: [finished], timeout: 1.0)
  }
}

final class TestMerge3: XCTestCase {
  private func assertSequencesAreMerged(first: [Int], second: [Int], third: [Int]) async {
    let merged = merge(first.async, second.async, third.async)
    var collected = [Int]()
    let expected = Set(first + second + third).sorted()
    
    var iterator = merged.makeAsyncIterator()
    while let item = await iterator.next() {
      collected.append(item)
    }
    let pastEnd = await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(Set(collected).sorted(), expected)
  }
  
  private func assertMergedSequenceThrowsWhenFirstThrows(first: [Int], second: [Int], third: [Int], throwOn value: Int) async throws {
    let merged = merge(first.async.map { try throwOn(value, $0) }, second.async, third.async)
    var collected = Set<Int>()
    let expected = Set(first.prefix(while: { $0 != value }))

    var iterator = merged.makeAsyncIterator()
    do {
      while let item = try await iterator.next() {
        collected.insert(item)
      }
      XCTFail("Merged sequence should throw after collecting elements from the first sequence up to \(value)")
    } catch {
      XCTAssertEqual(Failure(), error as? Failure)
    }
    let pastEnd = try await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(collected.intersection(expected), expected)
  }
  
  private func assertMergedSequenceThrowsWhenSecondThrows(first: [Int], second: [Int], third: [Int], throwOn value: Int) async throws {
    let merged = merge(first.async, second.async.map { try throwOn(value, $0) }, third.async)
    var collected = Set<Int>()
    let expected = Set(second.prefix(while: { $0 != value }))

    var iterator = merged.makeAsyncIterator()
    do {
      while let item = try await iterator.next() {
        collected.insert(item)
      }
      XCTFail("Merged sequence should throw after collecting elements from the second sequence up to \(value)")
    } catch {
      XCTAssertEqual(Failure(), error as? Failure)
    }
    let pastEnd = try await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(collected.intersection(expected), expected)
  }
  
  private func assertMergedSequenceThrowsWhenThirdThrows(first: [Int], second: [Int], third: [Int], throwOn value: Int) async throws {
    let merged = merge(first.async, second.async, third.async.map { try throwOn(value, $0) })
    var collected = Set<Int>()
    let expected = Set(third.prefix(while: { $0 != value }))

    var iterator = merged.makeAsyncIterator()
    do {
      while let item = try await iterator.next() {
        collected.insert(item)
      }
      XCTFail("Merged sequence should throw after collecting elements from the third sequence up to \(value)")
    } catch {
      XCTAssertEqual(Failure(), error as? Failure)
    }
    let pastEnd = try await iterator.next()
    
    XCTAssertNil(pastEnd)
    XCTAssertEqual(collected.intersection(expected), expected)
  }
  
  func test_merge_makes_sequence_with_elements_from_sources_when_all_have_same_size() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6],
      third: [7, 8, 9]
    )
  }

  func test_merge_makes_sequence_with_elements_from_sources_when_first_is_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8],
      third: [9, 10, 11]
    )
  }

  func test_merge_makes_sequence_with_elements_from_sources_when_second_is_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6, 7, 8],
      third: [9, 10, 11]
    )
  }

  func test_merge_makes_sequence_with_elements_from_sources_when_third_is_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6],
      third: [7, 8, 9, 10, 11]
    )
  }
  
  func test_merge_makes_sequence_with_elements_from_sources_when_first_and_second_are_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8, 9],
      third: [10, 11]
    )
  }

  func test_merge_makes_sequence_with_elements_from_sources_when_first_and_third_are_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3, 4, 5],
      second: [6, 7],
      third: [8, 9, 10, 11]
    )
  }

  func test_merge_makes_sequence_with_elements_from_sources_when_second_and_third_are_longer() async {
    await assertSequencesAreMerged(
      first: [1, 2, 3],
      second: [4, 5, 6, 7],
      third: [8, 9, 10, 11]
    )
  }

  func test_merge_produces_three_elements_from_first_and_throws_when_first_is_longer_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenFirstThrows(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8],
      third: [9, 10, 11],
      throwOn: 4
    )
  }

  func test_merge_produces_three_elements_from_first_and_throws_when_first_is_shorter_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenFirstThrows(
      first: [1, 2, 3, 4, 5],
      second: [6, 7, 8, 9, 10, 11],
      third: [12, 13, 14],
      throwOn: 4
    )
  }

  func test_merge_produces_three_elements_from_second_and_throws_when_second_is_longer_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenSecondThrows(
      first: [1, 2, 3],
      second: [4, 5, 6, 7, 8],
      third: [9, 10, 11],
      throwOn: 7
    )
  }

  func test_merge_produces_three_elements_from_second_and_throws_when_second_is_shorter_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenSecondThrows(
      first: [1, 2, 3, 4, 5, 6, 7],
      second: [7, 8, 9, 10, 11],
      third: [12, 13, 14],
      throwOn: 10
    )
  }
  
  func test_merge_produces_three_elements_from_third_and_throws_when_third_is_longer_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenThirdThrows(
      first: [1, 2, 3],
      second: [4, 5, 6],
      third: [7, 8, 9, 10, 11],
      throwOn: 10
    )
  }

  func test_merge_produces_three_elements_from_third_and_throws_when_third_is_shorter_and_throws_after_three_elements() async throws {
    try await assertMergedSequenceThrowsWhenThirdThrows(
      first: [1, 2, 3, 4, 5, 6, 7],
      second: [7, 8, 9, 10, 11],
      third: [12, 13, 14, 15],
      throwOn: 15
    )
  }
  
  func test_merge_makes_sequence_with_ordered_elements_when_sources_follow_a_timeline() {
    validate {
      "a---e---|"
      "-b-d-f-h|"
      "--c---g-|"
      merge($0.inputs[0], $0.inputs[1], $0.inputs[2])
      "abcdefgh|"
    }
  }

  func test_merge_finishes_when_iteration_task_is_cancelled() async {
    let source1 = Indefinite(value: "test1")
    let source2 = Indefinite(value: "test2")
    let source3 = Indefinite(value: "test3")
    let sequence = merge(source1.async, source2.async, source3.async)
    let finished = expectation(description: "finished")
    let iterated = expectation(description: "iterated")
    let task = Task {
      var firstIteration = false
      for await _ in sequence {
        if !firstIteration {
          firstIteration = true
          iterated.fulfill()
        }
      }
      finished.fulfill()
    }
    // ensure the other task actually starts
    wait(for: [iterated], timeout: 1.0)
    // cancellation should ensure the loop finishes
    // without regards to the remaining underlying sequence
    task.cancel()
    wait(for: [finished], timeout: 1.0)
  }
}
