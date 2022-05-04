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

final class TestUnzip: XCTestCase {
  func test_unzip_makes_symmetrical_sequences_with_parts_from_tuples() async {
    var a = GatedSequence([(1, "a"), (2, "b"), (3, "c")])
    let finished = expectation(description: "finished")
    finished.expectedFulfillmentCount = 2

    let (seq1, seq2) = a.unzip()

    let validator1 = Validator<Int>()
    validator1.test(seq1) { iterator in
      let pastEnd = await iterator.next()
      XCTAssertNil(pastEnd)
      finished.fulfill()
    }

    let validator2 = Validator<String>()
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
    XCTAssertEqual(value2, ["a"])

    a.advance()
    value1 = await validator1.validate()
    XCTAssertEqual(value1, [1, 2])
    value2 = await validator2.validate()
    XCTAssertEqual(value2, ["a", "b"])

    a.advance()
    value1 = await validator1.validate()
    XCTAssertEqual(value1, [1, 2, 3])
    value2 = await validator2.validate()
    XCTAssertEqual(value2, ["a", "b", "c"])

    wait(for: [finished], timeout: 1.0)
    value1 = validator1.current
    value2 = validator2.current
    XCTAssertEqual(value1, [1, 2, 3])
    XCTAssertEqual(value2, ["a", "b", "c"])
  }
}