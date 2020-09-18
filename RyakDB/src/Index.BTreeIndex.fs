module RyakDB.Index.BTreeIndex

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File
open RyakDB.Storage.Page
open RyakDB.Table
open RyakDB.Table.SlottedPage
open RyakDB.Index
open RyakDB.Buffer.Buffer
open RyakDB.Buffer.TransactionBuffer
open RyakDB.Concurrency.TransactionConcurrency
open RyakDB.Recovery.TransactionRecovery

module BTreePageFormatter =
    let inline makeDefaultRecord schema (offsetMap: Map<string, int32>) buffer position =
        schema.Fields()
        |> List.iter (fun field ->
            buffer.SetValue (position + offsetMap.[field]) (schema.DbType field |> DbConstant.defaultConstant))

let newBTreePageFormatter schema flags =
    let offsetMap = SlottedPage.offsetMap schema
    fun buffer ->
        let mutable position = 0
        buffer.SetValue position (IntDbConstant 0)
        position <- position + 4
        flags
        |> List.iter (fun f ->
            buffer.SetValue position (BigIntDbConstant f)
            position <- position + 8)
        let slotSize = SlottedPage.slotSize schema
        for pos in position .. slotSize .. (buffer.BufferSize - slotSize) do
            BTreePageFormatter.makeDefaultRecord schema offsetMap buffer pos

type BTreePage =
    { GetCountOfRecords: unit -> int32
      SetCountOfRecords: int32 -> unit
      BlockId: BlockId
      GetVal: int32 -> string -> DbConstant
      SetVal: int32 -> string -> DbConstant -> unit
      GetFlag: int32 -> int64
      SetFlag: int32 -> int64 -> unit
      IsFull: unit -> bool
      IsGettingFull: unit -> bool
      Insert: int32 -> unit
      Delete: int32 -> unit
      TransferRecords: int32 -> BTreePage -> int32 -> int32 -> unit
      Split: int32 -> int64 list -> int64
      CopyRecord: int32 -> int32 -> unit
      SetValueUnchecked: int32 -> string -> DbConstant -> unit
      Close: unit -> unit }

module BTreePage =
    type BTreePageState =
        { TxBuffer: TransactionBuffer
          TxConcurrency: TransactionConcurrency
          TxRecovery: TransactionRecovery
          TxReadOnly: bool
          Schema: Schema
          BlockId: BlockId
          CurrentBuffer: Buffer
          CountOfFlags: int32
          CountOfRecords: int32 }

    let slotSize schema buffer =
        let size =
            schema.Fields()
            |> List.fold (fun size f -> size + (schema.DbType f |> Page.maxSize)) 0

        if size < 0 || size > buffer.BufferSize
        then failwith ("Slot size overflow:" + size.ToString())
        size

    let getValue (buffer: Buffer) offset dbType = buffer.GetVal offset dbType

    let setValue txRecovery (buffer: Buffer) offset value =
        txRecovery.LogSetVal buffer offset value
        |> buffer.SetVal offset value

    let setValueWithoutLogging (buffer: Buffer) offset value = buffer.SetVal offset value None

    let slotPosition headerSize slotSize slot = headerSize + slot * slotSize

    let fieldPosition headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName =
        (slotPosition headerSize slotSize slot)
        + offsetMap.[fieldName]

    let setValueUnchecked txRecovery
                          schema
                          buffer
                          headerSize
                          slotSize
                          (offsetMap: Map<string, int32>)
                          slot
                          fieldName
                          value
                          =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValue txRecovery buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let getCountOfRecords buffer =
        getValue buffer 0 IntDbType |> DbConstant.toInt

    let setCountOfRecords txRecovery buffer value =
        IntDbConstant value
        |> setValue txRecovery buffer 0

    let setCountOfRecordsWithoutLogging buffer value =
        IntDbConstant value
        |> setValueWithoutLogging buffer 0

    let getVal schema buffer headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName =
        if slot >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slot.ToString())
        schema.DbType fieldName
        |> getValue buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let setVal txRecovery
               schema
               buffer
               headerSize
               slotSize
               countOfSlots
               (offsetMap: Map<string, int32>)
               slot
               fieldName
               value
               =
        if slot >= countOfSlots
        then failwith ("Slot overflow:" + slot.ToString())
        if slot >= getCountOfRecords buffer
        then failwith ("Slot overflow:" + slot.ToString())
        setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap slot fieldName value

    let setValWithoutLogging schema buffer headerSize slotSize (offsetMap: Map<string, int32>) slot fieldName value =
        DbConstant.castTo (schema.DbType fieldName) value
        |> setValueWithoutLogging buffer (fieldPosition headerSize slotSize offsetMap slot fieldName)

    let getFlag buffer no =
        getValue buffer (4 + 8 * no) BigIntDbType
        |> DbConstant.toLong

    let setFlag txRecovery buffer no value =
        BigIntDbConstant value
        |> setValue txRecovery buffer (4 + 8 * no)

    let isFull buffer headerSize slotSize =
        (getCountOfRecords buffer)
        + 1
        |> slotPosition headerSize slotSize
        >= buffer.BufferSize

    let isGettingFull buffer headerSize slotSize =
        (getCountOfRecords buffer)
        + 2
        |> slotPosition headerSize slotSize
        >= buffer.BufferSize

    let copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlot toSlot =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlot field
            |> setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap toSlot field)

    let copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap fromSlot toSlot =
        schema.Fields()
        |> List.iter (fun field ->
            getVal schema buffer headerSize slotSize offsetMap fromSlot field
            |> setValWithoutLogging schema buffer headerSize slotSize offsetMap toSlot field)

    let insert schema buffer headerSize slotSize offsetMap countOfSlots slot =
        buffer.LockFlushing(fun () ->
            if slot >= countOfSlots
            then failwith ("Slot overflow:" + slot.ToString())

            let countOfRecords = getCountOfRecords buffer
            if countOfRecords + 1 > countOfSlots
            then failwith ("Slot overflow:" + slot.ToString())

            for i in countOfRecords .. -1 .. slot + 1 do
                copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap (i - 1) i
            setCountOfRecordsWithoutLogging buffer (countOfRecords + 1))

    let delete schema buffer headerSize slotSize offsetMap slot =
        buffer.LockFlushing(fun () ->
            let countOfRecords = getCountOfRecords buffer
            for i in slot + 1 .. countOfRecords - 1 do
                copyRecordWithoutLogging schema buffer headerSize slotSize offsetMap i (i - 1)
            setCountOfRecordsWithoutLogging buffer (countOfRecords - 1))

    let transferRecords txRecovery schema buffer headerSize slotSize offsetMap start destPage destStart count =
        let countOfRecords = getCountOfRecords buffer

        let minCount =
            System.Math.Min(count, countOfRecords - start)

        let destCountOfRecords = destPage.GetCountOfRecords()
        for i in 0 .. destCountOfRecords - 1 do
            destPage.CopyRecord (destStart + i) (destStart + minCount + i)
        for i in 0 .. minCount - 1 do
            schema.Fields()
            |> List.iter (fun field ->
                getVal schema buffer headerSize slotSize offsetMap (start + i) field
                |> destPage.SetValueUnchecked (destStart + i) field)
        for i in 0 .. destCountOfRecords - 2 do
            if (start + minCount + i) < countOfRecords
            then copyRecord txRecovery schema buffer headerSize slotSize offsetMap (start + minCount + i) (start + i)

        setCountOfRecords txRecovery buffer (countOfRecords - minCount)
        destPage.SetCountOfRecords(destCountOfRecords + minCount)

    let appendBlock txBuffer txConcurrency schema (BlockId (filename, _)) flags =
        txConcurrency.ModifyFile filename

        let buffer =
            newBTreePageFormatter schema flags
            |> txBuffer.PinNew filename

        txBuffer.Unpin buffer
        buffer.BlockId()

    let split txBuffer
              txConcurrency
              txRecovery
              schema
              blockId
              buffer
              headerSize
              slotSize
              offsetMap
              newBTreePage
              splitSlot
              flags
              =
        let countOfRecords = getCountOfRecords buffer

        let newBlockId =
            appendBlock txBuffer txConcurrency schema blockId flags

        let newPage =
            newBTreePage txBuffer txConcurrency txRecovery schema newBlockId (List.length flags)

        transferRecords
            txRecovery
            schema
            buffer
            headerSize
            slotSize
            offsetMap
            splitSlot
            newPage
            0
            (countOfRecords - splitSlot)
        newPage.Close()
        let (BlockId (_, blockNo)) = newBlockId
        blockNo

    let close txBuffer buffer = txBuffer.Unpin buffer

let rec newBTreePage txBuffer txConcurrency txRecovery schema blockId countOfFlags =
    let buffer = txBuffer.Pin blockId
    let offsetMap = SlottedPage.offsetMap schema
    let slotSize = BTreePage.slotSize schema buffer

    let countOfSlots =
        (buffer.BufferSize - countOfFlags * 8) / slotSize

    let headerSize = 4 + countOfFlags * 8

    { GetCountOfRecords = fun () -> BTreePage.getCountOfRecords buffer
      SetCountOfRecords = fun value -> BTreePage.setCountOfRecords txRecovery buffer value
      BlockId = blockId
      GetVal = fun slot fieldName -> BTreePage.getVal schema buffer headerSize slotSize offsetMap slot fieldName
      SetVal =
          fun slot fieldName value ->
              BTreePage.setVal txRecovery schema buffer headerSize slotSize countOfSlots offsetMap slot fieldName value
      GetFlag = fun no -> BTreePage.getFlag buffer no
      SetFlag = fun no value -> BTreePage.setFlag txRecovery buffer no value
      IsFull = fun () -> BTreePage.isFull buffer headerSize slotSize
      IsGettingFull = fun () -> BTreePage.isGettingFull buffer headerSize slotSize
      Insert = fun slot -> BTreePage.insert schema buffer headerSize slotSize offsetMap countOfSlots slot
      Delete = fun slot -> BTreePage.delete schema buffer headerSize slotSize offsetMap slot
      TransferRecords =
          fun start destPage destStart count ->
              BTreePage.transferRecords
                  txRecovery
                  schema
                  buffer
                  headerSize
                  slotSize
                  offsetMap
                  start
                  destPage
                  destStart
                  count
      Split =
          fun splitSlot flags ->
              BTreePage.split
                  txBuffer
                  txConcurrency
                  txRecovery
                  schema
                  blockId
                  buffer
                  headerSize
                  slotSize
                  offsetMap
                  newBTreePage
                  splitSlot
                  flags
      CopyRecord =
          fun fromSlot toSlot ->
              BTreePage.copyRecord txRecovery schema buffer headerSize slotSize offsetMap fromSlot toSlot
      SetValueUnchecked =
          fun slot fieldName value ->
              BTreePage.setValueUnchecked txRecovery schema buffer headerSize slotSize offsetMap slot fieldName value
      Close = fun () -> BTreePage.close txBuffer buffer }

type BTreeBranchEntry = BTreeBranchEntry of key: SearchKey * blockNo: int64

let inline newBTreeBranchEntry key blockNo = BTreeBranchEntry(key, blockNo)

type SearchPurpose =
    | Read
    | Insert
    | Delete

type BTreeBranch =
    { GetCountOfRecords: unit -> int32
      BranchesMayBeUpdated: unit -> BlockId list
      Search: string -> SearchKey -> SearchPurpose -> BlockId
      Insert: BTreeBranchEntry -> BTreeBranchEntry option
      MakeNewRoot: BTreeBranchEntry -> unit
      Close: unit -> unit }

module BTreeBranch =
    let insertASlot blockId keyType slot = ()
    let deleteASlot blockId keyType slot = ()

    let inline getFileName indexName = indexName + "_branch.idx"

    let inline keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField "child" BigIntDbType
        types
        |> List.iteri (fun i t -> sch.AddField ("key" + i.ToString()) t)
        sch

    let getKey page slot (SearchKeyType (keys)) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slot ("key" + i.ToString()))
        |> SearchKey.newSearchKey

    let getChildBlockNo page slot =
        page.GetVal slot "child" |> DbConstant.toLong

    let insertSlot txRecovery keyType page (SearchKey keys) blockNo slot =
        txRecovery.LogIndexPageInsertion false page.BlockId keyType slot
        |> ignore
        page.Insert slot
        page.SetVal slot "child" (BigIntDbConstant blockNo)
        keys
        |> List.iteri (fun i k -> page.SetVal slot ("key" + i.ToString()) k)

    let getLevelFlag page = page.GetFlag 0

    let setLevelFlag page value = page.SetFlag 0 value

    let findMatchingSlot keyType (page: BTreePage) searchKey =
        let rec loopSlot startSlot endSlot =
            let middleSlot = (startSlot + endSlot) / 2
            if startSlot <> middleSlot then
                let key = getKey page middleSlot keyType
                if key < searchKey then loopSlot middleSlot endSlot else loopSlot startSlot middleSlot
            else
                startSlot, endSlot

        let endSlot = page.GetCountOfRecords() - 1
        if endSlot < 0 then
            0
        else
            let startSlot, endSlot = loopSlot 0 endSlot
            let key = getKey page endSlot keyType
            if key <= searchKey then endSlot else startSlot

    let findChildBlockNo keyType page searchKey =
        findMatchingSlot keyType page searchKey
        |> getChildBlockNo page

    let searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec loopSearch currentPage childBlockNo branchesMayBeUpdated =
            if getLevelFlag currentPage > 0L then
                let (BlockId (pagefile, _)) = currentPage.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForModification childBlockId

                let childPage =
                    newBTreePage txBuffer txConcurrency txRecovery schema childBlockId 1

                let branchesMayBeUpdated =
                    if childPage.IsGettingFull() then
                        childBlockId :: branchesMayBeUpdated
                    else
                        branchesMayBeUpdated
                        |> List.iter txConcurrency.CrabBackBranchBlockForModification
                        [ childBlockId ]

                currentPage.Close()
                loopSearch childPage (findChildBlockNo keyType childPage searchKey) branchesMayBeUpdated
            else
                currentPage, childBlockNo, branchesMayBeUpdated

        txConcurrency.CrabDownBranchBlockForModification page.BlockId

        let currentPage, childBlockNo, branchesMayBeUpdated =
            loopSearch page (findChildBlockNo keyType page searchKey) [ page.BlockId ]

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ModifyLeafBlock leafBlockId
        leafBlockId, currentPage, branchesMayBeUpdated |> List.rev

    let searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey =
        let rec loopSearch currentPage childBlockNo =
            if getLevelFlag currentPage > 0L then
                let (BlockId (pagefile, _)) = currentPage.BlockId
                let childBlockId = BlockId.newBlockId pagefile childBlockNo
                txConcurrency.CrabDownBranchBlockForRead childBlockId

                let childPage =
                    newBTreePage txBuffer txConcurrency txRecovery schema childBlockId 1

                txConcurrency.CrabBackBranchBlockForRead currentPage.BlockId
                currentPage.Close()
                loopSearch childPage (findChildBlockNo keyType childPage searchKey)
            else
                currentPage, childBlockNo

        txConcurrency.CrabDownBranchBlockForRead page.BlockId

        let currentPage, childBlockNo =
            loopSearch page (findChildBlockNo keyType page searchKey)

        let leafBlockId =
            BlockId.newBlockId leafFileName childBlockNo

        txConcurrency.ReadLeafBlock leafBlockId
        txConcurrency.CrabDownBranchBlockForRead currentPage.BlockId
        leafBlockId, currentPage, []

    let search txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey purpose =
        match purpose with
        | Insert -> searchForInsert txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey
        | Delete
        | Read -> searchForRead txBuffer txConcurrency txRecovery schema keyType page leafFileName searchKey

    let insert txRecovery keyType (page: BTreePage) (BTreeBranchEntry (key, blockNo)) =
        let newSlot =
            if page.GetCountOfRecords() > 0 then (findMatchingSlot keyType page key) + 1 else 0

        insertSlot txRecovery keyType page key blockNo newSlot
        if page.IsFull() then
            let splitPos = page.GetCountOfRecords() / 2
            let splitKey = getKey page splitPos keyType

            let splitBlockNo =
                page.Split splitPos [ getLevelFlag page ]

            Some(newBTreeBranchEntry splitKey splitBlockNo)
        else
            None

    let makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry =
        let (BlockId (fileName, blockNo)) = page.BlockId

        let currentPage =
            if blockNo <> 0L then
                page.Close()
                newBTreePage txBuffer txConcurrency txRecovery schema (BlockId.newBlockId fileName 0L) 1
            else
                page

        let firstKey = getKey currentPage 0 keyType
        let level = getLevelFlag currentPage
        let splitBlockNo = currentPage.Split 0 [ level ]

        let oldRoot =
            newBTreeBranchEntry firstKey splitBlockNo

        insert txRecovery keyType currentPage oldRoot
        |> ignore
        insert txRecovery keyType currentPage entry
        |> ignore
        setLevelFlag currentPage (level + 1L)
        currentPage

let newBTreeBranch txBuffer txConcurrency txRecovery blockId keyType =
    let schema = BTreeBranch.keyTypeToSchema keyType

    let mutable page =
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 1

    let mutable branchesMayBeUpdated = []

    { GetCountOfRecords = fun () -> page.GetCountOfRecords()
      BranchesMayBeUpdated = fun () -> branchesMayBeUpdated
      Search =
          fun leafFileName searchKey purpose ->
              let leafBlockId, nextPage, nextMayBeUpdated =
                  BTreeBranch.search
                      txBuffer
                      txConcurrency
                      txRecovery
                      schema
                      keyType
                      page
                      leafFileName
                      searchKey
                      purpose

              page <- nextPage
              branchesMayBeUpdated <- nextMayBeUpdated
              leafBlockId
      Insert = fun entry -> BTreeBranch.insert txRecovery keyType page entry
      MakeNewRoot =
          fun entry -> page <- BTreeBranch.makeNewRoot txBuffer txConcurrency txRecovery schema keyType page entry
      Close =
          fun () ->
              page.Close()
              branchesMayBeUpdated <- [] }

type BTreeLeaf =
    { Next: unit -> bool
      GetDataRecordId: unit -> RecordId
      Insert: RecordId -> BTreeBranchEntry option
      Delete: RecordId -> unit
      Close: unit -> unit }

module BTreeLeaf =
    type BTreeLeafState =
        { CurrentPage: BTreePage
          CurrentSlot: int32
          IsOverflowing: bool
          OverflowFrom: int64
          MoveFrom: int64 }

    let insertASlot blockId keyType slot = ()
    let deleteASlot blockId keyType slot = ()

    let inline getFileName indexName = indexName + "_leaf.idx"

    let inline keyTypeToSchema (SearchKeyType types) =
        let sch = Schema.newSchema ()
        sch.AddField "blockNo" BigIntDbType
        sch.AddField "slotNo" IntDbType
        types
        |> List.iteri (fun i t -> sch.AddField ("key" + i.ToString()) t)
        sch

    let getKey page slot (SearchKeyType (keys)) =
        keys
        |> List.mapi (fun i _ -> page.GetVal slot ("key" + i.ToString()))
        |> SearchKey.newSearchKey

    let getDataRecordId filename page slot =
        RecordId.newBlockRecordId
            (page.GetVal slot "slotNo" |> DbConstant.toInt)
            filename
            (page.GetVal slot "blockNo" |> DbConstant.toLong)

    let insertSlot txRecovery keyType page (SearchKey keys) (RecordId (slotNo, BlockId (_, blockNo))) slot =
        txRecovery.LogIndexPageInsertion false page.BlockId keyType slot
        |> ignore
        page.Insert slot
        page.SetVal slot "blockNo" (BigIntDbConstant blockNo)
        page.SetVal slot "slotNo" (IntDbConstant slotNo)
        keys
        |> List.iteri (fun i k -> page.SetVal slot ("key" + i.ToString()) k)

    let deleteSlot txRecovery keyType page slot =
        txRecovery.LogIndexPageDeletion false page.BlockId keyType slot
        |> ignore
        page.Delete slot

    let getOverflowFlag page = page.GetFlag 0

    let setOverflowFlag page value = page.SetFlag 0 value

    let getSiblingFlag page = page.GetFlag 1

    let setSiblingFlag page value = page.SetFlag 1 value

    let moveSlotBefore keyType searchRange page =
        let searchMin = searchRange.GetMin()

        let rec loopSlot startSlot endSlot =
            let middleSlot = (startSlot + endSlot) / 2
            if startSlot <> middleSlot then
                if getKey page middleSlot keyType < searchMin
                then loopSlot middleSlot endSlot
                else loopSlot startSlot middleSlot
            else
                startSlot, endSlot

        let endSlot = page.GetCountOfRecords() - 1
        if endSlot < 0 then
            -1
        else
            let startSlot, endSlot = loopSlot 0 endSlot
            if getKey page endSlot keyType < searchMin then endSlot
            else if getKey page startSlot keyType < searchMin then startSlot
            else startSlot - 1

    let moveTo txBuffer txConcurrency txRecovery schema currentPage blockNo =
        let (BlockId (filename, _)) = currentPage.BlockId
        let blockId = BlockId.newBlockId filename blockNo
        txConcurrency.ReadLeafBlock blockId
        currentPage.Close()
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 2

    let next txBuffer txConcurrency txRecovery schema keyType searchRange state =
        let rec loopNext (currentPage: BTreePage) currentSlot isOverflowing overflowFrom moveFrom =
            let slot = currentSlot + 1
            if isOverflowing then
                if slot >= currentPage.GetCountOfRecords() then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId

                    let page =
                        getOverflowFlag currentPage
                        |> moveTo txBuffer txConcurrency txRecovery schema currentPage

                    let (BlockId (_, nextBlockNo)) = page.BlockId

                    let overflowing, ofFrom =
                        if nextBlockNo = overflowFrom then false, -1L else isOverflowing, overflowFrom

                    true,
                    { CurrentPage = page
                      CurrentSlot = 0
                      IsOverflowing = overflowing
                      OverflowFrom = ofFrom
                      MoveFrom = currentBlockNo }
                else
                    true,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else if slot >= currentPage.GetCountOfRecords() then
                let sibling = getSiblingFlag currentPage
                if sibling <> -1L then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId
                    loopNext
                        (moveTo txBuffer txConcurrency txRecovery schema currentPage sibling)
                        -1
                        isOverflowing
                        overflowFrom
                        currentBlockNo
                else
                    false,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else if getKey currentPage slot keyType
                    |> searchRange.MatchsKey then
                let overflow = getOverflowFlag currentPage
                if slot = 0 && overflow <> -1L then
                    let (BlockId (_, currentBlockNo)) = currentPage.BlockId
                    true,
                    { CurrentPage = moveTo txBuffer txConcurrency txRecovery schema currentPage overflow
                      CurrentSlot = 0
                      IsOverflowing = true
                      OverflowFrom = currentBlockNo
                      MoveFrom = currentBlockNo }
                else
                    true,
                    { CurrentPage = currentPage
                      CurrentSlot = slot
                      IsOverflowing = isOverflowing
                      OverflowFrom = overflowFrom
                      MoveFrom = moveFrom }
            else if getKey currentPage slot keyType
                    |> searchRange.BetweenMinAndMax then
                loopNext currentPage slot isOverflowing overflowFrom moveFrom
            else
                false,
                { CurrentPage = currentPage
                  CurrentSlot = slot
                  IsOverflowing = isOverflowing
                  OverflowFrom = overflowFrom
                  MoveFrom = moveFrom }

        loopNext state.CurrentPage state.CurrentSlot state.IsOverflowing state.OverflowFrom state.MoveFrom

    let insert txRecovery keyType searchRange recordId state =
        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let searchKey = searchRange.ToSearchKey()
        let slot = state.CurrentSlot + 1
        insertSlot txRecovery keyType state.CurrentPage searchKey recordId slot

        let overflow = getOverflowFlag state.CurrentPage
        let splitKey = getKey state.CurrentPage 1 keyType
        if slot = 0
           && overflow <> -1L
           && splitKey <> searchKey then
            let newBlockNo =
                state.CurrentPage.Split
                    1
                    [ getOverflowFlag state.CurrentPage
                      getSiblingFlag state.CurrentPage ]

            setOverflowFlag state.CurrentPage -1L
            setSiblingFlag state.CurrentPage newBlockNo
            Some(newBTreeBranchEntry splitKey newBlockNo), slot
        else

        if state.CurrentPage.IsFull() then
            let firstKey = getKey state.CurrentPage 0 keyType

            let lastKey =
                getKey state.CurrentPage (state.CurrentPage.GetCountOfRecords() - 1) keyType

            if lastKey = firstKey then
                let (BlockId (_, currentBlockNo)) = state.CurrentPage.BlockId
                let overflow = getOverflowFlag state.CurrentPage

                let newBlockNo =
                    state.CurrentPage.Split
                        1
                        [ if overflow = -1L then
                            currentBlockNo
                          else
                              overflow
                              -1L ]

                setOverflowFlag state.CurrentPage newBlockNo
                None, slot
            else
                let mutable splitPos =
                    state.CurrentPage.GetCountOfRecords() / 2

                let splitKey =
                    getKey state.CurrentPage splitPos keyType

                let splitKey =
                    if splitKey = firstKey then
                        while splitKey = getKey state.CurrentPage splitPos keyType do
                            splitPos <- splitPos + 1
                        getKey state.CurrentPage splitPos keyType
                    else
                        while splitKey = getKey state.CurrentPage (splitPos - 1) keyType do
                            splitPos <- splitPos - 1
                        splitKey

                let newBlockNo =
                    state.CurrentPage.Split
                        splitPos
                        [ -1L
                          getSiblingFlag state.CurrentPage ]

                setSiblingFlag state.CurrentPage newBlockNo
                Some(newBTreeBranchEntry splitKey newBlockNo), slot
        else
            None, slot

    let delete txBuffer txConcurrency txRecovery dataFileName schema keyType searchRange recordId state =
        let rec loopDelete state =
            let result, newstate =
                next txBuffer txConcurrency txRecovery schema keyType searchRange state

            if result then
                if recordId = getDataRecordId dataFileName newstate.CurrentPage newstate.CurrentSlot then
                    deleteSlot txRecovery keyType newstate.CurrentPage newstate.CurrentSlot
                    newstate
                else
                    loopDelete newstate
            else
                newstate

        if not (searchRange.IsSingleValue()) then failwith "Not supported"
        let state = loopDelete state
        if state.IsOverflowing then
            if state.CurrentPage.GetCountOfRecords() = 0 then
                let (BlockId (filename, _)) = state.CurrentPage.BlockId

                let prePage =
                    newBTreePage txBuffer txConcurrency txRecovery schema (BlockId.newBlockId filename state.MoveFrom) 2

                let overflow = getOverflowFlag state.CurrentPage
                let (BlockId (_, blockNo)) = prePage.BlockId
                setOverflowFlag prePage (if overflow = blockNo then -1L else overflow)
                prePage.Close()
        else
            let overflow = getOverflowFlag state.CurrentPage
            if overflow <> -1L then
                let (BlockId (filename, blockNo)) = state.CurrentPage.BlockId
                let blockId = BlockId.newBlockId filename overflow
                txConcurrency.ModifyLeafBlock blockId

                let overflowPage =
                    newBTreePage txBuffer txConcurrency txRecovery schema blockId 2

                let firstKey = getKey state.CurrentPage 0 keyType
                if state.CurrentPage.GetCountOfRecords() = 0
                   || (overflowPage.GetCountOfRecords()
                       <> 0
                       && firstKey <> getKey overflowPage 0 keyType) then
                    overflowPage.TransferRecords (overflowPage.GetCountOfRecords() - 1) state.CurrentPage 0 1
                    if overflowPage.GetCountOfRecords() = 0 then
                        let overflow = getOverflowFlag overflowPage
                        setOverflowFlag state.CurrentPage (if overflow = blockNo then -1L else overflow)
                    overflowPage.Close()
        state

let newBTreeLeaf txBuffer txConcurrency txRecovery dataFileName blockId keyType searchRange =
    let schema = BTreeLeaf.keyTypeToSchema keyType

    let page =
        newBTreePage txBuffer txConcurrency txRecovery schema blockId 2

    let mutable state: BTreeLeaf.BTreeLeafState =
        { CurrentPage = page
          CurrentSlot = BTreeLeaf.moveSlotBefore keyType searchRange page
          IsOverflowing = false
          OverflowFrom = -1L
          MoveFrom = -1L }

    { Next =
          fun () ->
              let result, newstate =
                  BTreeLeaf.next txBuffer txConcurrency txRecovery schema keyType searchRange state

              state <- newstate
              result
      GetDataRecordId = fun () -> BTreeLeaf.getDataRecordId dataFileName page state.CurrentSlot
      Insert =
          fun recordId ->
              let result, nextSlot =
                  BTreeLeaf.insert txRecovery keyType searchRange recordId state

              state <- { state with CurrentSlot = nextSlot }
              result
      Delete =
          fun recordId ->
              state <-
                  BTreeLeaf.delete
                      txBuffer
                      txConcurrency
                      txRecovery
                      dataFileName
                      schema
                      keyType
                      searchRange
                      recordId
                      state
      Close = fun () -> page.Close() }

module BTreeIndex =
    type BTreeIndexState =
        { Leaf: BTreeLeaf
          BranchesMayBeUpdated: BlockId list }

    let inline fileSize (fileMgr: FileManager) txConcurrency fileName =
        txConcurrency.ReadFile fileName
        fileMgr.Size fileName

    let appendBlock txBuffer txConcurrency fileName schema flags =
        txConcurrency.ModifyFile fileName

        let buff =
            txBuffer.PinNew fileName (newBTreePageFormatter schema flags)

        txBuffer.Unpin buff
        buff.BlockId

    let inline createRoot txBuffer txConcurrency txRecovery keyType branchFileName =
        newBTreeBranch txBuffer txConcurrency txRecovery (BlockId.newBlockId branchFileName 0L) keyType

    let search txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange purpose =
        let root =
            createRoot txBuffer txConcurrency txRecovery keyType branchFileName

        let leafBlockId =
            root.Search leafFileName (searchRange.GetMin()) purpose

        let branchesMayBeUpdated =
            if purpose = Insert then root.BranchesMayBeUpdated() else []

        root.Close()

        let leaf =
            newBTreeLeaf txBuffer txConcurrency txRecovery indexInfo.TableInfo.FileName leafBlockId keyType searchRange

        { Leaf = leaf
          BranchesMayBeUpdated = branchesMayBeUpdated }

    let preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName =
        let branchSize =
            fileSize fileMgr txConcurrency branchFileName

        for i in 0L .. branchSize - 1L do
            BlockId.newBlockId branchFileName i
            |> txBuffer.Pin
            |> ignore

        let leafSize =
            fileSize fileMgr txConcurrency leafFileName

        for i in 0L .. leafSize - 1L do
            BlockId.newBlockId leafFileName i
            |> txBuffer.Pin
            |> ignore

    let inline beforeFirst txBuffer txConcurrency txRecovery indexInfo keyType branchFileName leafFileName searchRange =
        if searchRange.IsValid()
        then Some
                 (search
                     txBuffer
                      txConcurrency
                      txRecovery
                      indexInfo
                      keyType
                      branchFileName
                      leafFileName
                      searchRange
                      Read)
        else None

    let inline next state =
        match state with
        | Some ({ Leaf = leaf }) -> leaf.Next()
        | _ -> failwith "Must call beforeFirst()"

    let inline getDataRecordId state =
        match state with
        | Some ({ Leaf = leaf }) -> leaf.GetDataRecordId()
        | _ -> failwith "Must call beforeFirst()"

    let insert txBuffer
               txConcurrency
               txRecovery
               txReadOnly
               indexInfo
               keyType
               branchFileName
               leafFileName
               doLogicalLogging
               key
               dataRecordId
               =
        let inline insertEntry entry branchBlockId =
            match entry with
            | Some (e) ->
                let branch =
                    newBTreeBranch txBuffer txConcurrency txRecovery branchBlockId keyType

                let newEntry = branch.Insert e
                branch.Close()
                newEntry
            | _ -> None

        if txReadOnly then failwith "Transaction read only"

        let newstate =
            search
                txBuffer
                txConcurrency
                txRecovery
                indexInfo
                keyType
                branchFileName
                leafFileName
                (SearchRange.newSearchRangeBySearchKey key)
                Insert

        let entry = newstate.Leaf.Insert dataRecordId
        newstate.Leaf.Close()
        if Option.isSome entry then
            if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

            match newstate.BranchesMayBeUpdated
                  |> List.fold insertEntry entry with
            | Some (e) ->
                let root =
                    createRoot txBuffer txConcurrency txRecovery keyType branchFileName

                root.MakeNewRoot e
                root.Close()
            | _ -> ()

            if doLogicalLogging then
                let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
                txRecovery.LogIndexInsertionEnd indexInfo.IndexName key blockNo slotNo
                |> ignore

    let delete txBuffer
               txConcurrency
               txRecovery
               txReadOnly
               indexInfo
               keyType
               branchFileName
               leafFileName
               doLogicalLogging
               key
               dataRecordId
               =
        if txReadOnly then failwith "Transaction read only"

        let newstate =
            search
                txBuffer
                txConcurrency
                txRecovery
                indexInfo
                keyType
                branchFileName
                leafFileName
                (SearchRange.newSearchRangeBySearchKey key)
                Delete

        if doLogicalLogging then txRecovery.LogLogicalStart() |> ignore

        newstate.Leaf.Delete dataRecordId

        if doLogicalLogging then
            let (RecordId (slotNo, BlockId (_, blockNo))) = dataRecordId
            txRecovery.LogIndexDeletionEnd indexInfo.IndexName key blockNo slotNo
            |> ignore

    let inline close state =
        state |> Option.iter (fun s -> s.Leaf.Close())

let newBTreeIndex fileMgr txBuffer txConcurrency txRecovery txReadOnly indexInfo keyType =
    let leafFileName =
        BTreeLeaf.getFileName indexInfo.IndexName

    if BTreeIndex.fileSize fileMgr txConcurrency leafFileName = 0L then
        BTreeIndex.appendBlock txBuffer txConcurrency leafFileName (BTreeLeaf.keyTypeToSchema keyType) [ -1L; -1L ]
        |> ignore

    let branchFileName =
        BTreeBranch.getFileName indexInfo.IndexName

    if BTreeIndex.fileSize fileMgr txConcurrency branchFileName = 0L then
        BTreeIndex.appendBlock txBuffer txConcurrency branchFileName (BTreeBranch.keyTypeToSchema keyType) [ 0L ]
        |> ignore

    let root =
        BTreeIndex.createRoot txBuffer txConcurrency txRecovery keyType branchFileName

    if root.GetCountOfRecords() = 0 then
        root.Insert(newBTreeBranchEntry (SearchKeyType.getMin keyType) 0L)
        |> ignore
    root.Close()

    let mutable state = None
    { BeforeFirst =
          fun searchRange ->
              BTreeIndex.close state
              state <-
                  BTreeIndex.beforeFirst
                      txBuffer
                      txConcurrency
                      txRecovery
                      indexInfo
                      keyType
                      branchFileName
                      leafFileName
                      searchRange
      Next = fun () -> BTreeIndex.next state
      GetDataRecordId = fun () -> BTreeIndex.getDataRecordId state
      Insert =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close state
              state <- None
              BTreeIndex.insert
                  txBuffer
                  txConcurrency
                  txRecovery
                  txReadOnly
                  indexInfo
                  keyType
                  branchFileName
                  leafFileName
                  doLogicalLogging
                  key
                  dataRecordId
      Delete =
          fun doLogicalLogging key dataRecordId ->
              BTreeIndex.close state
              state <- None
              BTreeIndex.delete
                  txBuffer
                  txConcurrency
                  txRecovery
                  txReadOnly
                  indexInfo
                  keyType
                  branchFileName
                  leafFileName
                  doLogicalLogging
                  key
                  dataRecordId
      Close =
          fun () ->
              BTreeIndex.close state
              state <- None
      PreLoadToMemory = fun () -> BTreeIndex.preLoadToMemory fileMgr txBuffer txConcurrency branchFileName leafFileName }
