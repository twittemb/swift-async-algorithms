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

public extension AsyncSequence {
  func unzip<A, B>() -> (AsyncMapSequence<AsyncSplit2Sequence<Self>, A>, AsyncMapSequence<AsyncSplit2Sequence<Self>, B>) where Element == (A, B) {
    let (seq1, seq2) = self.split()
    return (seq1.map { $0.0 }, seq2.map { $0.1 })
  }
}