module RyakDB.Test.Concurrency.LockTableTest

open Xunit
open FsUnit.Xunit
open RyakDB.Storage
open RyakDB.Concurrency.LockTable

let init max =
    let filename =
        "test_locktable_"
        + System.DateTime.Now.Ticks.ToString()

    Array.init max (fun i -> BlockIdLockerKey(BlockId.newBlockId filename (int64 i)))

[<Fact>]
let ``S lock`` () =
    let lockTbl = newLockTable 1000
    let blocks = init 100
    let txNo1 = 12341L
    let txNo2 = 12342L

    blocks
    |> Array.iter (fun block ->
        lockTbl.SLock txNo1 block
        lockTbl.SLock txNo1 block
        lockTbl.SLock txNo2 block
        lockTbl.SLock txNo1 block)

    lockTbl.ReleaseAll txNo1 false
    lockTbl.ReleaseAll txNo2 false

[<Fact>]
let ``X lock`` () =
    let lockTbl = newLockTable 1000
    let blocks = init 5
    let txNo1 = 123451L
    let txNo2 = 123452L

    lockTbl.XLock txNo1 blocks.[0]
    lockTbl.XLock txNo1 blocks.[0]
    lockTbl.SLock txNo1 blocks.[0]
    lockTbl.SLock txNo1 blocks.[0]

    lockTbl.SLock txNo1 blocks.[1]
    lockTbl.SLock txNo1 blocks.[1]
    lockTbl.XLock txNo1 blocks.[1]
    lockTbl.XLock txNo1 blocks.[1]

    shouldFail (fun () -> lockTbl.SLock txNo2 blocks.[0])
    |> ignore
    shouldFail (fun () -> lockTbl.SLock txNo2 blocks.[1])
    |> ignore
    shouldFail (fun () -> lockTbl.XLock txNo2 blocks.[0])
    |> ignore
    shouldFail (fun () -> lockTbl.XLock txNo2 blocks.[1])
    |> ignore

    lockTbl.ReleaseAll txNo1 false

    lockTbl.XLock txNo2 blocks.[0]
    lockTbl.XLock txNo2 blocks.[1]

    lockTbl.ReleaseAll txNo2 false

[<Fact>]
let ``SIX lock`` () =
    let lockTbl = newLockTable 1000
    let blocks = init 5
    let txNo1 = 123456781L
    let txNo2 = 123456782L

    lockTbl.SLock txNo1 blocks.[0]
    shouldFail (fun () -> lockTbl.SIXLock txNo2 blocks.[0])
    |> ignore

    lockTbl.SIXLock txNo1 blocks.[1]
    shouldFail (fun () -> lockTbl.SIXLock txNo2 blocks.[1])
    |> ignore

    lockTbl.ReleaseAll txNo1 false
    lockTbl.ReleaseAll txNo2 false

[<Fact>]
let ``IS lock`` () =
    let lockTbl = newLockTable 1000
    let blocks = init 10
    let txNo1 = 1234561L
    let txNo2 = 1234562L

    blocks
    |> Array.iter (fun block -> lockTbl.ISLock txNo1 block)

    lockTbl.SLock txNo1 blocks.[0]
    lockTbl.ISLock txNo1 blocks.[1]
    lockTbl.XLock txNo1 blocks.[2]
    lockTbl.IXLock txNo1 blocks.[3]
    lockTbl.SIXLock txNo1 blocks.[4]

    lockTbl.SLock txNo2 blocks.[5]
    lockTbl.ISLock txNo2 blocks.[6]
    shouldFail (fun () -> lockTbl.XLock txNo2 blocks.[7])
    |> ignore
    lockTbl.IXLock txNo2 blocks.[8]
    lockTbl.SIXLock txNo2 blocks.[9]

    lockTbl.ReleaseAll txNo1 false
    lockTbl.ReleaseAll txNo2 false

[<Fact>]
let ``IX lock`` () =
    let lockTbl = newLockTable 1000
    let blocks = init 5
    let txNo1 = 12345671L
    let txNo2 = 12345672L

    lockTbl.SLock txNo1 blocks.[0]
    shouldFail (fun () -> lockTbl.IXLock txNo2 blocks.[0])
    |> ignore

    lockTbl.IXLock txNo1 blocks.[1]
    lockTbl.IXLock txNo2 blocks.[1]

    lockTbl.IXLock txNo1 blocks.[2]
    shouldFail (fun () -> lockTbl.SLock txNo2 blocks.[2])
    |> ignore

    lockTbl.IXLock txNo1 blocks.[3]
    shouldFail (fun () -> lockTbl.XLock txNo2 blocks.[3])
    |> ignore

    lockTbl.ReleaseAll txNo1 false
    lockTbl.ReleaseAll txNo2 false
