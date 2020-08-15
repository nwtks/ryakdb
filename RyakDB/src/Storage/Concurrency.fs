namespace RyakDB.Storage.Concurrency

open RyakDB.Storage.Type
open RyakDB.Storage.File

type LockerKey =
    | FileNameLockerKey of string
    | BlockIdLockerKey of BlockId
    | RecordIdLockerKey of RecordId

type LockTable =
    { SLock: int64 -> LockerKey -> unit
      XLock: int64 -> LockerKey -> unit
      SIXLock: int64 -> LockerKey -> unit
      ISLock: int64 -> LockerKey -> unit
      IXLock: int64 -> LockerKey -> unit
      ReleaseSLock: int64 -> LockerKey -> unit
      ReleaseXLock: int64 -> LockerKey -> unit
      ReleaseSIXLock: int64 -> LockerKey -> unit
      ReleaseISLock: int64 -> LockerKey -> unit
      ReleaseIXLock: int64 -> LockerKey -> unit
      ReleaseAll: int64 -> bool -> unit
      LocktableNotifier: unit -> unit -> unit }

module LockTable =
    type LockType =
        | SLock
        | XLock
        | SIXLock
        | ISLock
        | IXLock

    type Lockers =
        { SLockers: Set<int64>
          XLocker: int64
          SIXLocker: int64
          ISLockers: Set<int64>
          IXLockers: Set<int64>
          RequestSet: Set<int64> }

    type LockTableState =
        { LockerMap: System.Collections.Concurrent.ConcurrentDictionary<LockerKey, Lockers>
          LockByTxMap: System.Collections.Concurrent.ConcurrentDictionary<int64, System.Collections.Concurrent.ConcurrentDictionary<LockerKey, bool>>
          TxnsToBeAborted: System.Collections.Concurrent.ConcurrentDictionary<int64, bool>
          TxWaitMap: System.Collections.Concurrent.ConcurrentDictionary<int64, obj>
          ToBeNotified: System.Collections.Concurrent.BlockingCollection<int64>
          Anchors: obj []
          WaitTime: int32 }

    let private waitingTooLong (timestamp: System.DateTime) waitTime =
        (System.DateTime.Now.Ticks - timestamp.Ticks)
        / System.TimeSpan.TicksPerMillisecond
        + 50L > (int64 waitTime)

    let private prepareAnchor (anchors: obj []) a =
        let h = hash a % anchors.Length
        if h < 0 then h + anchors.Length else h
        |> anchors.GetValue

    let prepareLockers (lockerMap: System.Collections.Concurrent.ConcurrentDictionary<LockerKey, Lockers>) key =
        lockerMap.GetOrAdd
            (key,
             (fun _ ->
                 { SLockers = Set.empty
                   XLocker = -1L
                   SIXLocker = -1L
                   ISLockers = Set.empty
                   IXLockers = Set.empty
                   RequestSet = Set.empty }))

    let getLockSet
        (lockByTxMap: System.Collections.Concurrent.ConcurrentDictionary<int64, System.Collections.Concurrent.ConcurrentDictionary<LockerKey, bool>>)
        txNo
        =
        lockByTxMap.GetOrAdd(txNo, (fun _ -> System.Collections.Concurrent.ConcurrentDictionary()))

    let isSLocked lockers = not (lockers.SLockers.IsEmpty)

    let isXLocked lockers = lockers.XLocker <> -1L

    let isSIXLocked lockers = lockers.SIXLocker <> -1L

    let isISLocked lockers = not (lockers.ISLockers.IsEmpty)

    let isIXLocked lockers = not (lockers.IXLockers.IsEmpty)

    let hasSLock lockers txNo = lockers.SLockers.Contains txNo

    let hasXLock lockers txNo = lockers.XLocker = txNo

    let hasSIXLock lockers txNo = lockers.SIXLocker = txNo

    let hasISLock lockers txNo = lockers.ISLockers.Contains txNo

    let hasIXLock lockers txNo = lockers.IXLockers.Contains txNo

    let isTheOnlySLocker lockers txNo =
        lockers.SLockers.Count = 1
        && lockers.SLockers.Contains txNo

    let isTheOnlyISLocker lockers txNo =
        lockers.ISLockers.Count = 1
        && lockers.ISLockers.Contains txNo

    let isTheOnlyIXLocker lockers txNo =
        lockers.IXLockers.Count = 1
        && lockers.IXLockers.Contains txNo

    let isSLockable lockers txNo =
        (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let isXLockable lockers txNo =
        (not (isSLocked lockers)
         || isTheOnlySLocker lockers txNo)
        && (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isISLocked lockers)
            || isTheOnlyISLocker lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let isSIXLockable lockers txNo =
        (not (isSLocked lockers)
         || isTheOnlySLocker lockers txNo)
        && (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let isISLockable lockers txNo =
        not (isXLocked lockers) || hasXLock lockers txNo

    let isIXLockable lockers txNo =
        (not (isSLocked lockers)
         || isTheOnlySLocker lockers txNo)
        && (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)

    let avoidDeadlock state lockType lockers txNo =
        if state.TxnsToBeAborted.ContainsKey txNo then
            failwith
                ("abort tx."
                 + txNo.ToString()
                 + " for preventing deadlock")
        if lockType = IXLock
           || lockType = SIXLock
           || lockType = XLock then
            lockers.SLockers
            |> Seq.filter (fun n -> n > txNo)
            |> Seq.iter (fun n -> if state.TxnsToBeAborted.TryAdd(n, true) then state.ToBeNotified.Add n)
        if lockType = SLock
           || lockType = SIXLock
           || lockType = XLock then
            lockers.IXLockers
            |> Seq.filter (fun n -> n > txNo)
            |> Seq.iter (fun n -> if state.TxnsToBeAborted.TryAdd(n, true) then state.ToBeNotified.Add n)
        if lockType = XLock then
            lockers.ISLockers
            |> Seq.filter (fun n -> n > txNo)
            |> Seq.iter (fun n -> if state.TxnsToBeAborted.TryAdd(n, true) then state.ToBeNotified.Add n)
        if lockType = IXLock
           || lockType = SLock
           || lockType = SIXLock
           || lockType = XLock then
            if lockers.SIXLocker > txNo then
                if state.TxnsToBeAborted.TryAdd(lockers.SIXLocker, true)
                then state.ToBeNotified.Add lockers.SIXLocker
        if lockers.XLocker > txNo then
            if state.TxnsToBeAborted.TryAdd(lockers.XLocker, true)
            then state.ToBeNotified.Add lockers.XLocker

    let createLock state lockType isLockable hasLock addTxNoToLockers =
        fun txNo key ->
            let anchor = prepareAnchor state.Anchors key
            state.TxWaitMap.TryAdd(txNo, anchor) |> ignore
            lock anchor (fun () ->
                let lockers = prepareLockers state.LockerMap key
                if not (hasLock lockers txNo) then
                    let timestamp = System.DateTime.Now
                    let mutable loopLockers = lockers
                    while not (isLockable loopLockers txNo)
                          && not (waitingTooLong timestamp state.WaitTime) do
                        avoidDeadlock state lockType loopLockers txNo
                        loopLockers <-
                            { loopLockers with
                                  RequestSet = lockers.RequestSet.Add txNo }
                        System.Threading.Monitor.Wait(anchor, state.WaitTime)
                        |> ignore
                        loopLockers <-
                            { loopLockers with
                                  RequestSet = lockers.RequestSet.Remove txNo }
                    if not (isLockable loopLockers txNo) then failwith "Lock abort"
                    let newLockers = addTxNoToLockers txNo loopLockers
                    (getLockSet state.LockByTxMap txNo).TryAdd(key, true)
                    |> ignore
                    state.LockerMap.TryUpdate(key, newLockers, lockers)
                    |> ignore)
            state.TxWaitMap.TryRemove txNo |> ignore

    let sLock state =
        createLock state SLock isSLockable hasSLock (fun txNo lockers ->
            { lockers with
                  SLockers = lockers.SLockers.Add txNo })

    let xLock state =
        createLock state XLock isXLockable hasXLock (fun txNo lockers -> { lockers with XLocker = txNo })

    let sixLock state =
        createLock state SIXLock isSIXLockable hasSIXLock (fun txNo lockers -> { lockers with SIXLocker = txNo })

    let isLock state =
        createLock state ISLock isISLockable hasISLock (fun txNo lockers ->
            { lockers with
                  ISLockers = lockers.ISLockers.Add txNo })

    let ixLock state =
        createLock state IXLock isIXLockable hasIXLock (fun txNo lockers ->
            { lockers with
                  IXLockers = lockers.IXLockers.Add txNo })

    let releaseLock lockers anchor lockType txNo =
        System.Threading.Monitor.PulseAll anchor
        match lockType with
        | XLock ->
            if lockers.XLocker = txNo then
                let newLockers = { lockers with XLocker = -1L }
                System.Threading.Monitor.PulseAll anchor
                newLockers
            else
                lockers
        | SIXLock ->
            if lockers.SIXLocker = txNo then
                let newLockers = { lockers with SIXLocker = -1L }
                System.Threading.Monitor.PulseAll anchor
                newLockers
            else
                lockers
        | SLock ->
            if lockers.SLockers.Contains txNo then
                let newLockers =
                    { lockers with
                          SLockers = lockers.SLockers.Remove txNo }

                if newLockers.SLockers.IsEmpty
                then System.Threading.Monitor.PulseAll anchor
                newLockers
            else
                lockers
        | ISLock ->
            if lockers.ISLockers.Contains txNo then
                let newLockers =
                    { lockers with
                          ISLockers = lockers.ISLockers.Remove txNo }

                if newLockers.ISLockers.IsEmpty
                then System.Threading.Monitor.PulseAll anchor
                newLockers
            else
                lockers
        | IXLock ->
            if lockers.IXLockers.Contains txNo then
                let newLockers =
                    { lockers with
                          IXLockers = lockers.IXLockers.Remove txNo }

                if newLockers.IXLockers.IsEmpty
                then System.Threading.Monitor.PulseAll anchor
                newLockers
            else
                lockers

    let release state lockType txNo key =
        let anchor = prepareAnchor state.Anchors key
        lock anchor (fun () ->
            let mutable lockers = Unchecked.defaultof<Lockers>
            if state.LockerMap.TryGetValue(key, &lockers) then
                let newLockers = releaseLock lockers anchor lockType txNo
                if not (hasSLock newLockers txNo)
                   && not (hasXLock newLockers txNo)
                   && not (hasSIXLock newLockers txNo)
                   && not (hasISLock newLockers txNo)
                   && not (hasIXLock newLockers txNo) then
                    (getLockSet state.LockByTxMap txNo).TryRemove key
                    |> ignore
                    if not (isSLocked newLockers)
                       && not (isXLocked newLockers)
                       && not (isSIXLocked newLockers)
                       && not (isISLocked newLockers)
                       && not (isIXLocked newLockers)
                       && newLockers.RequestSet.IsEmpty then
                        state.LockerMap.TryRemove key |> ignore
                    else
                        state.LockerMap.TryUpdate(key, newLockers, lockers)
                        |> ignore
                else
                    state.LockerMap.TryUpdate(key, newLockers, lockers)
                    |> ignore)

    let releaseSLock state = release state SLock

    let releaseXLock state = release state XLock

    let releaseSIXLock state = release state SIXLock

    let releaseISLock state = release state ISLock

    let releaseIXLock state = release state IXLock

    let releaseAll state txNo sLockOnly =
        getLockSet state.LockByTxMap txNo
        |> Seq.iter (fun e ->
            let anchor = prepareAnchor state.Anchors e.Key
            lock anchor (fun () ->
                let mutable lockers = Unchecked.defaultof<Lockers>
                if state.LockerMap.TryGetValue(e.Key, &lockers) then
                    if hasSLock lockers txNo
                    then lockers <- releaseLock lockers anchor SLock txNo
                    if hasXLock lockers txNo && not sLockOnly
                    then lockers <- releaseLock lockers anchor XLock txNo
                    if hasSIXLock lockers txNo
                    then lockers <- releaseLock lockers anchor SIXLock txNo
                    while hasISLock lockers txNo do
                        lockers <- releaseLock lockers anchor ISLock txNo
                    while hasIXLock lockers txNo && not sLockOnly do
                        lockers <- releaseLock lockers anchor IXLock txNo
                    if not (isSLocked lockers)
                       && not (isXLocked lockers)
                       && not (isSIXLocked lockers)
                       && not (isISLocked lockers)
                       && not (isIXLocked lockers)
                       && lockers.RequestSet.IsEmpty then
                        state.LockerMap.TryRemove e.Key |> ignore))
        state.TxWaitMap.TryRemove txNo |> ignore
        state.TxnsToBeAborted.TryRemove txNo |> ignore
        state.LockByTxMap.TryRemove txNo |> ignore

    let locktableNotifier state =
        fun () ->
            while true do
                let txNo = state.ToBeNotified.Take()
                let mutable anchor = Unchecked.defaultof<_>
                if state.TxWaitMap.TryGetValue(txNo, &anchor)
                then lock anchor (fun () -> System.Threading.Monitor.PulseAll anchor)

    let newLockTable waitTime =
        let state =
            { LockerMap = System.Collections.Concurrent.ConcurrentDictionary()
              LockByTxMap = System.Collections.Concurrent.ConcurrentDictionary()
              TxnsToBeAborted = System.Collections.Concurrent.ConcurrentDictionary()
              TxWaitMap = System.Collections.Concurrent.ConcurrentDictionary()
              ToBeNotified = new System.Collections.Concurrent.BlockingCollection<int64>()
              Anchors = Array.init 1019 (fun _ -> obj ())
              WaitTime = waitTime }

        { SLock = sLock state
          XLock = xLock state
          SIXLock = sixLock state
          ISLock = isLock state
          IXLock = ixLock state
          ReleaseSLock = releaseSLock state
          ReleaseXLock = releaseXLock state
          ReleaseSIXLock = releaseSIXLock state
          ReleaseISLock = releaseISLock state
          ReleaseIXLock = releaseIXLock state
          ReleaseAll = releaseAll state
          LocktableNotifier = fun () -> locktableNotifier state }

module ConcurrencyManager =
    let newReadCommitted txNo lockTable =
        let mutable toReleaseSLockAtEndStatement: LockerKey list = []

        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.ReleaseISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
                  toReleaseSLockAtEndStatement <-
                      (RecordIdLockerKey recordId)
                      :: toReleaseSLockAtEndStatement
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock =
              fun blockId ->
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId) },
        { OnTxCommit = fun _ -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun _ -> lockTable.ReleaseAll txNo false
          OnTxEndStatement =
              fun _ ->
                  List.rev toReleaseSLockAtEndStatement
                  |> List.iter (fun key -> lockTable.ReleaseSLock txNo key)
                  toReleaseSLockAtEndStatement <- [] }

    let newRepeatableRead txNo lockTable =
        let mutable toReleaseSLockAtEndStatement: LockerKey list = []

        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.ReleaseISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex =
              fun fileName ->
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ReleaseISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock =
              fun blockId ->
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
                  toReleaseSLockAtEndStatement <-
                      (BlockIdLockerKey blockId)
                      :: toReleaseSLockAtEndStatement
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId) },
        { OnTxCommit = fun _ -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun _ -> lockTable.ReleaseAll txNo false
          OnTxEndStatement =
              fun _ ->
                  List.rev toReleaseSLockAtEndStatement
                  |> List.iter (fun key -> lockTable.ReleaseSLock txNo key)
                  toReleaseSLockAtEndStatement <- [] }

    let newSerializable txNo lockTable =
        { ModifyFile = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadFile = fun fileName -> lockTable.ISLock txNo (FileNameLockerKey fileName)
          InsertBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.XLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ModifyBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadBlock =
              fun blockId ->
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.SLock txNo (BlockIdLockerKey blockId)
          ModifyRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.IXLock txNo (FileNameLockerKey fileName)
                  lockTable.IXLock txNo (BlockIdLockerKey blockId)
                  lockTable.XLock txNo (RecordIdLockerKey recordId)
          ReadRecord =
              fun recordId ->
                  let (RecordId (_, blockId)) = recordId
                  let (BlockId (fileName, _)) = blockId
                  lockTable.ISLock txNo (FileNameLockerKey fileName)
                  lockTable.ISLock txNo (BlockIdLockerKey blockId)
                  lockTable.SLock txNo (RecordIdLockerKey recordId)
          ModifyIndex = fun fileName -> lockTable.XLock txNo (FileNameLockerKey fileName)
          ReadIndex = fun fileName -> lockTable.ISLock txNo (FileNameLockerKey fileName)
          ModifyLeafBlock = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReadLeafBlock = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForModification = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          CrabDownDirBlockForRead = fun blockId -> lockTable.SLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForModification = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId)
          CrabBackDirBlockForRead = fun blockId -> lockTable.ReleaseSLock txNo (BlockIdLockerKey blockId)
          LockRecordFileHeader = fun blockId -> lockTable.XLock txNo (BlockIdLockerKey blockId)
          ReleaseRecordFileHeader = fun blockId -> lockTable.ReleaseXLock txNo (BlockIdLockerKey blockId) },
        { OnTxCommit = fun _ -> lockTable.ReleaseAll txNo false
          OnTxRollback = fun _ -> lockTable.ReleaseAll txNo false
          OnTxEndStatement = fun _ -> () }

    let newConcurrencyManager lockTable txNo isolationLevel =
        match isolationLevel with
        | ReadCommitted -> newReadCommitted txNo lockTable
        | RepeatableRead -> newRepeatableRead txNo lockTable
        | Serializable -> newSerializable txNo lockTable
