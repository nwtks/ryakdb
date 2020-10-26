module RyakDB.Concurrency.LockService

open RyakDB.Storage
open RyakDB.Table

type LockerKey =
    | FileNameLockerKey of string
    | BlockIdLockerKey of BlockId
    | RecordIdLockerKey of RecordId

type LockService =
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
      LockNotifier: unit -> unit -> unit }

module LockService =
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
          RequestSet: System.Collections.Concurrent.ConcurrentDictionary<int64, bool> }

    type LockServiceState =
        { LockerMap: System.Collections.Concurrent.ConcurrentDictionary<LockerKey, Lockers>
          LockerKeySetMap: System.Collections.Concurrent.ConcurrentDictionary<int64, System.Collections.Concurrent.ConcurrentDictionary<LockerKey, bool>>
          TxNosToBeAborted: System.Collections.Concurrent.ConcurrentDictionary<int64, bool>
          TxNosToBeNotified: System.Collections.Concurrent.BlockingCollection<int64>
          TxAnchorMap: System.Collections.Concurrent.ConcurrentDictionary<int64, obj>
          Anchors: obj []
          WaitTime: int32 }

    let private waitingTooLong (timestamp: System.DateTime) waitTime =
        (System.DateTime.Now.Ticks - timestamp.Ticks)
        / System.TimeSpan.TicksPerMillisecond
        + 50L > (int64 waitTime)

    let private getAnchor a anchors =
        let h = hash a % Array.length anchors
        if h < 0 then h + Array.length anchors else h
        |> anchors.GetValue

    let getLockers key (lockerMap: System.Collections.Concurrent.ConcurrentDictionary<LockerKey, Lockers>) =
        lockerMap.GetOrAdd
            (key,
             (fun _ ->
                 { SLockers = Set.empty
                   XLocker = -1L
                   SIXLocker = -1L
                   ISLockers = Set.empty
                   IXLockers = Set.empty
                   RequestSet = System.Collections.Concurrent.ConcurrentDictionary() }))

    let getLockerKeySet txNo
                        (lockerKeySetMap: System.Collections.Concurrent.ConcurrentDictionary<int64, System.Collections.Concurrent.ConcurrentDictionary<LockerKey, bool>>)
                        =
        lockerKeySetMap.GetOrAdd(txNo, (fun _ -> System.Collections.Concurrent.ConcurrentDictionary()))

    let isSLocked lockers = lockers.SLockers |> Set.isEmpty |> not

    let isXLocked lockers = lockers.XLocker <> -1L

    let isSIXLocked lockers = lockers.SIXLocker <> -1L

    let isISLocked lockers = lockers.ISLockers |> Set.isEmpty |> not

    let isIXLocked lockers = lockers.IXLockers |> Set.isEmpty |> not

    let hasSLock txNo lockers = lockers.SLockers |> Set.contains txNo

    let hasXLock txNo lockers = lockers.XLocker = txNo

    let hasSIXLock txNo lockers = lockers.SIXLocker = txNo

    let hasISLock txNo lockers = lockers.ISLockers |> Set.contains txNo

    let hasIXLock txNo lockers = lockers.IXLockers |> Set.contains txNo

    let isTheOnlySLocker txNo lockers =
        lockers.SLockers
        |> Set.count = 1
        && lockers.SLockers |> Set.contains txNo

    let isTheOnlyISLocker txNo lockers =
        lockers.ISLockers
        |> Set.count = 1
        && lockers.ISLockers |> Set.contains txNo

    let isTheOnlyIXLocker txNo lockers =
        lockers.IXLockers
        |> Set.count = 1
        && lockers.IXLockers |> Set.contains txNo

    let isSLockable txNo lockers =
        (lockers
         |> isXLocked
         |> not
         || lockers |> hasXLock txNo)
        && (lockers
            |> isSIXLocked
            |> not
            || lockers |> hasSIXLock txNo)
        && (lockers
            |> isIXLocked
            |> not
            || lockers |> isTheOnlyIXLocker txNo)

    let isXLockable txNo lockers =
        (lockers
         |> isSLocked
         |> not
         || lockers |> isTheOnlySLocker txNo)
        && (lockers
            |> isXLocked
            |> not
            || lockers |> hasXLock txNo)
        && (lockers
            |> isSIXLocked
            |> not
            || lockers |> hasSIXLock txNo)
        && (lockers
            |> isISLocked
            |> not
            || lockers |> isTheOnlyISLocker txNo)
        && (lockers
            |> isIXLocked
            |> not
            || lockers |> isTheOnlyIXLocker txNo)

    let isSIXLockable txNo lockers =
        (lockers
         |> isSLocked
         |> not
         || lockers |> isTheOnlySLocker txNo)
        && (lockers
            |> isXLocked
            |> not
            || lockers |> hasXLock txNo)
        && (lockers
            |> isSIXLocked
            |> not
            || lockers |> hasSIXLock txNo)
        && (lockers
            |> isIXLocked
            |> not
            || lockers |> isTheOnlyIXLocker txNo)

    let isISLockable txNo lockers =
        lockers
        |> isXLocked
        |> not
        || lockers |> hasXLock txNo

    let isIXLockable txNo lockers =
        (lockers
         |> isSLocked
         |> not
         || lockers |> isTheOnlySLocker txNo)
        && (lockers
            |> isXLocked
            |> not
            || lockers |> hasXLock txNo)
        && (lockers
            |> isSIXLocked
            |> not
            || lockers |> hasSIXLock txNo)

    let avoidDeadlock state lockType txNo lockers =
        if state.TxNosToBeAborted.ContainsKey txNo then
            failwith
                ("abort tx:"
                 + txNo.ToString()
                 + " for preventing deadlock")

        if lockType = IXLock
           || lockType = SIXLock
           || lockType = XLock then
            lockers.SLockers
            |> Set.filter ((>) txNo)
            |> Set.iter (fun no ->
                if state.TxNosToBeAborted.TryAdd(no, true)
                then state.TxNosToBeNotified.Add no)

        if lockType = SLock
           || lockType = SIXLock
           || lockType = XLock then
            lockers.IXLockers
            |> Set.filter ((>) txNo)
            |> Set.iter (fun no ->
                if state.TxNosToBeAborted.TryAdd(no, true)
                then state.TxNosToBeNotified.Add no)

        if lockType = XLock then
            lockers.ISLockers
            |> Set.filter ((>) txNo)
            |> Set.iter (fun no ->
                if state.TxNosToBeAborted.TryAdd(no, true)
                then state.TxNosToBeNotified.Add no)

        if lockType = IXLock
           || lockType = SLock
           || lockType = SIXLock
           || lockType = XLock then
            if lockers.SIXLocker > txNo then
                if state.TxNosToBeAborted.TryAdd(lockers.SIXLocker, true)
                then state.TxNosToBeNotified.Add lockers.SIXLocker

        if lockers.XLocker > txNo then
            if state.TxNosToBeAborted.TryAdd(lockers.XLocker, true)
            then state.TxNosToBeNotified.Add lockers.XLocker

    let createLock state lockType isLockable hasLock addTxNoToLockers =
        let rec waitLock anchor timestamp txNo lockers =
            if lockers
               |> isLockable txNo
               |> not
               && waitingTooLong timestamp state.WaitTime |> not then
                lockers |> avoidDeadlock state lockType txNo
                lockers.RequestSet.TryAdd(txNo, true) |> ignore
                System.Threading.Monitor.Wait(anchor, state.WaitTime)
                |> ignore
                lockers.RequestSet.TryRemove(txNo) |> ignore
                waitLock anchor timestamp txNo lockers
            else
                lockers

        fun txNo key ->
            let anchor = state.Anchors |> getAnchor key
            state.TxAnchorMap.TryAdd(txNo, anchor) |> ignore
            lock anchor (fun () ->
                let lockers = state.LockerMap |> getLockers key
                if lockers |> hasLock txNo |> not then
                    if lockers
                       |> waitLock anchor System.DateTime.Now txNo
                       |> isLockable txNo
                       |> not then
                        failwith "Lock abort"
                    (state.LockerKeySetMap |> getLockerKeySet txNo).TryAdd(key, true)
                    |> ignore
                    state.LockerMap.TryUpdate(key, lockers |> addTxNoToLockers txNo, lockers)
                    |> ignore)
            state.TxAnchorMap.TryRemove txNo |> ignore

    let sLock state =
        createLock state SLock isSLockable hasSLock (fun txNo lockers ->
            { lockers with
                  SLockers = lockers.SLockers |> Set.add txNo })

    let xLock state =
        createLock state XLock isXLockable hasXLock (fun txNo lockers -> { lockers with XLocker = txNo })

    let sixLock state =
        createLock state SIXLock isSIXLockable hasSIXLock (fun txNo lockers -> { lockers with SIXLocker = txNo })

    let isLock state =
        createLock state ISLock isISLockable hasISLock (fun txNo lockers ->
            { lockers with
                  ISLockers = lockers.ISLockers |> Set.add txNo })

    let ixLock state =
        createLock state IXLock isIXLockable hasIXLock (fun txNo lockers ->
            { lockers with
                  IXLockers = lockers.IXLockers |> Set.add txNo })

    let releaseLock anchor lockType txNo lockers =
        System.Threading.Monitor.PulseAll anchor

        match lockType with
        | XLock ->
            if lockers.XLocker = txNo then
                let unlockedLockers = { lockers with XLocker = -1L }
                System.Threading.Monitor.PulseAll anchor
                unlockedLockers
            else
                lockers
        | SIXLock ->
            if lockers.SIXLocker = txNo then
                let unlockedLockers = { lockers with SIXLocker = -1L }
                System.Threading.Monitor.PulseAll anchor
                unlockedLockers
            else
                lockers
        | SLock ->
            if lockers.SLockers |> Set.contains txNo then
                let unlockedLockers =
                    { lockers with
                          SLockers = lockers.SLockers |> Set.remove txNo }

                if unlockedLockers.SLockers |> Set.isEmpty
                then System.Threading.Monitor.PulseAll anchor

                unlockedLockers
            else
                lockers
        | ISLock ->
            if lockers.ISLockers |> Set.contains txNo then
                let unlockedLockers =
                    { lockers with
                          ISLockers = lockers.ISLockers |> Set.remove txNo }

                if unlockedLockers.ISLockers |> Set.isEmpty
                then System.Threading.Monitor.PulseAll anchor

                unlockedLockers
            else
                lockers
        | IXLock ->
            if lockers.IXLockers |> Set.contains txNo then
                let unlockedLockers =
                    { lockers with
                          IXLockers = lockers.IXLockers |> Set.remove txNo }

                if unlockedLockers.IXLockers |> Set.isEmpty
                then System.Threading.Monitor.PulseAll anchor

                unlockedLockers
            else
                lockers

    let release state lockType txNo key =
        let anchor = state.Anchors |> getAnchor key
        lock anchor (fun () ->
            let mutable lockers = Unchecked.defaultof<Lockers>
            if state.LockerMap.TryGetValue(key, &lockers) then
                let releasedLockers =
                    lockers |> releaseLock anchor lockType txNo

                if releasedLockers
                   |> hasSLock txNo
                   |> not
                   && releasedLockers |> hasXLock txNo |> not
                   && releasedLockers |> hasSIXLock txNo |> not
                   && releasedLockers |> hasISLock txNo |> not
                   && releasedLockers |> hasIXLock txNo |> not then
                    (state.LockerKeySetMap |> getLockerKeySet txNo).TryRemove key
                    |> ignore
                    if releasedLockers
                       |> isSLocked
                       |> not
                       && releasedLockers |> isXLocked |> not
                       && releasedLockers |> isSIXLocked |> not
                       && releasedLockers |> isISLocked |> not
                       && releasedLockers |> isIXLocked |> not
                       && releasedLockers.RequestSet.IsEmpty then
                        state.LockerMap.TryRemove key |> ignore
                    else
                        state.LockerMap.TryUpdate(key, releasedLockers, lockers)
                        |> ignore
                else
                    state.LockerMap.TryUpdate(key, releasedLockers, lockers)
                    |> ignore)

    let releaseSLock state = release state SLock

    let releaseXLock state = release state XLock

    let releaseSIXLock state = release state SIXLock

    let releaseISLock state = release state ISLock

    let releaseIXLock state = release state IXLock

    let releaseAll state txNo sLockOnly =
        state.LockerKeySetMap
        |> getLockerKeySet txNo
        |> Seq.map (fun e -> e.Key)
        |> Seq.iter (fun key ->
            let anchor = state.Anchors |> getAnchor key
            lock anchor (fun () ->
                let mutable lockers = Unchecked.defaultof<Lockers>
                if state.LockerMap.TryGetValue(key, &lockers) then
                    while lockers |> hasSLock txNo do
                        lockers <- lockers |> releaseLock anchor SLock txNo

                    while lockers |> hasXLock txNo && not sLockOnly do
                        lockers <- lockers |> releaseLock anchor XLock txNo

                    while lockers |> hasSIXLock txNo do
                        lockers <- lockers |> releaseLock anchor SIXLock txNo

                    while lockers |> hasISLock txNo do
                        lockers <- lockers |> releaseLock anchor ISLock txNo

                    while lockers |> hasIXLock txNo && not sLockOnly do
                        lockers <- lockers |> releaseLock anchor IXLock txNo

                    if lockers
                       |> isSLocked
                       |> not
                       && lockers |> isXLocked |> not
                       && lockers |> isSIXLocked |> not
                       && lockers |> isISLocked |> not
                       && lockers |> isIXLocked |> not
                       && lockers.RequestSet.IsEmpty then
                        state.LockerMap.TryRemove key |> ignore))
        state.TxAnchorMap.TryRemove txNo |> ignore
        state.TxNosToBeAborted.TryRemove txNo |> ignore
        state.LockerKeySetMap.TryRemove txNo |> ignore

    let lockNotifier state =
        let rec notify () =
            let txNo = state.TxNosToBeNotified.Take()
            let mutable anchor = Unchecked.defaultof<_>
            if state.TxAnchorMap.TryGetValue(txNo, &anchor)
            then lock anchor (fun () -> System.Threading.Monitor.PulseAll anchor)
            notify ()

        notify

let newLockService waitTime =
    let state: LockService.LockServiceState =
        { LockerMap = System.Collections.Concurrent.ConcurrentDictionary()
          LockerKeySetMap = System.Collections.Concurrent.ConcurrentDictionary()
          TxNosToBeAborted = System.Collections.Concurrent.ConcurrentDictionary()
          TxNosToBeNotified = new System.Collections.Concurrent.BlockingCollection<int64>()
          TxAnchorMap = System.Collections.Concurrent.ConcurrentDictionary()
          Anchors = Array.init 1019 (fun _ -> obj ())
          WaitTime = waitTime }

    { SLock = LockService.sLock state
      XLock = LockService.xLock state
      SIXLock = LockService.sixLock state
      ISLock = LockService.isLock state
      IXLock = LockService.ixLock state
      ReleaseSLock = LockService.releaseSLock state
      ReleaseXLock = LockService.releaseXLock state
      ReleaseSIXLock = LockService.releaseSIXLock state
      ReleaseISLock = LockService.releaseISLock state
      ReleaseIXLock = LockService.releaseIXLock state
      ReleaseAll = LockService.releaseAll state
      LockNotifier = fun () -> LockService.lockNotifier state }
