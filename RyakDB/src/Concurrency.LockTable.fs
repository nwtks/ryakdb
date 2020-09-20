module RyakDB.Concurrency.LockTable

open RyakDB.Storage
open RyakDB.Table

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

    let getLockSet (lockByTxMap: System.Collections.Concurrent.ConcurrentDictionary<int64, System.Collections.Concurrent.ConcurrentDictionary<LockerKey, bool>>)
                   txNo
                   =
        lockByTxMap.GetOrAdd(txNo, (fun _ -> System.Collections.Concurrent.ConcurrentDictionary()))

    let inline isSLocked lockers = not (lockers.SLockers.IsEmpty)

    let inline isXLocked lockers = lockers.XLocker <> -1L

    let inline isSIXLocked lockers = lockers.SIXLocker <> -1L

    let inline isISLocked lockers = not (lockers.ISLockers.IsEmpty)

    let inline isIXLocked lockers = not (lockers.IXLockers.IsEmpty)

    let inline hasSLock lockers txNo = lockers.SLockers.Contains txNo

    let inline hasXLock lockers txNo = lockers.XLocker = txNo

    let inline hasSIXLock lockers txNo = lockers.SIXLocker = txNo

    let inline hasISLock lockers txNo = lockers.ISLockers.Contains txNo

    let inline hasIXLock lockers txNo = lockers.IXLockers.Contains txNo

    let inline isTheOnlySLocker lockers txNo =
        lockers.SLockers.Count = 1
        && lockers.SLockers.Contains txNo

    let inline isTheOnlyISLocker lockers txNo =
        lockers.ISLockers.Count = 1
        && lockers.ISLockers.Contains txNo

    let inline isTheOnlyIXLocker lockers txNo =
        lockers.IXLockers.Count = 1
        && lockers.IXLockers.Contains txNo

    let inline isSLockable lockers txNo =
        (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let inline isXLockable lockers txNo =
        (not (isSLocked lockers)
         || isTheOnlySLocker lockers txNo)
        && (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isISLocked lockers)
            || isTheOnlyISLocker lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let inline isSIXLockable lockers txNo =
        (not (isSLocked lockers)
         || isTheOnlySLocker lockers txNo)
        && (not (isXLocked lockers) || hasXLock lockers txNo)
        && (not (isSIXLocked lockers)
            || hasSIXLock lockers txNo)
        && (not (isIXLocked lockers)
            || isTheOnlyIXLocker lockers txNo)

    let inline isISLockable lockers txNo =
        not (isXLocked lockers) || hasXLock lockers txNo

    let inline isIXLockable lockers txNo =
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
    let state: LockTable.LockTableState =
        { LockerMap = System.Collections.Concurrent.ConcurrentDictionary()
          LockByTxMap = System.Collections.Concurrent.ConcurrentDictionary()
          TxnsToBeAborted = System.Collections.Concurrent.ConcurrentDictionary()
          TxWaitMap = System.Collections.Concurrent.ConcurrentDictionary()
          ToBeNotified = new System.Collections.Concurrent.BlockingCollection<int64>()
          Anchors = Array.init 1019 (fun _ -> obj ())
          WaitTime = waitTime }

    { SLock = LockTable.sLock state
      XLock = LockTable.xLock state
      SIXLock = LockTable.sixLock state
      ISLock = LockTable.isLock state
      IXLock = LockTable.ixLock state
      ReleaseSLock = LockTable.releaseSLock state
      ReleaseXLock = LockTable.releaseXLock state
      ReleaseSIXLock = LockTable.releaseSIXLock state
      ReleaseISLock = LockTable.releaseISLock state
      ReleaseIXLock = LockTable.releaseIXLock state
      ReleaseAll = LockTable.releaseAll state
      LocktableNotifier = fun () -> LockTable.locktableNotifier state }
