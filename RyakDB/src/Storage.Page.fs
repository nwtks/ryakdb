module RyakDB.Storage.Page

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File

type Page =
    { GetVal: int32 -> SqlType -> SqlConstant
      SetVal: int32 -> SqlConstant -> unit
      Read: BlockId -> unit
      Write: BlockId -> unit
      Append: string -> BlockId }

module Page =
    let maxSize sqlType =
        if sqlType |> SqlType.isFixedSize then sqlType |> SqlType.maxSize else 4 + (sqlType |> SqlType.maxSize)

    let size sqlConstant =
        let sqlType = sqlConstant |> SqlConstant.sqlType
        if sqlType |> SqlType.isFixedSize then sqlType |> SqlType.maxSize else 4 + (sqlConstant |> SqlConstant.size)

    let getVal contents offset sqlType =
        let off, size =
            if sqlType |> SqlType.isFixedSize then
                offset, sqlType |> SqlType.maxSize
            else
                let bytes = contents |> FileBuffer.get offset 4
                offset + bytes.Length, System.BitConverter.ToInt32(System.ReadOnlySpan(bytes))

        contents
        |> FileBuffer.get off size
        |> SqlConstant.fromBytes sqlType

    let setVal fileMgr contents offset value =
        let bytes = value |> SqlConstant.toBytes

        let off =
            if value
               |> SqlConstant.sqlType
               |> SqlType.isFixedSize then
                offset
            else
                if offset + 4 + bytes.Length > fileMgr.BlockSize
                then failwith "Page buffer overflow"

                let sizebytes =
                    bytes.Length |> System.BitConverter.GetBytes

                contents |> FileBuffer.put offset sizebytes
                offset + sizebytes.Length

        contents |> FileBuffer.put off bytes

    let read (fileMgr: FileManager) contents blockId = fileMgr.Read contents blockId

    let write (fileMgr: FileManager) contents blockId = fileMgr.Write contents blockId

    let append (fileMgr: FileManager) contents fileName = fileMgr.Append contents fileName

    let newPage fileMgr =
        let contents =
            FileBuffer.newFileBuffer fileMgr.BlockSize

        { GetVal = fun offset sqlType -> lock contents (fun () -> getVal contents offset sqlType)
          SetVal = fun offset value -> lock contents (fun () -> setVal fileMgr contents offset value)
          Read = fun blockId -> lock contents (fun () -> read fileMgr contents blockId)
          Write = fun blockId -> lock contents (fun () -> write fileMgr contents blockId)
          Append = fun fileName -> lock contents (fun () -> append fileMgr contents fileName) }
