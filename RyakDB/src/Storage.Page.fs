module RyakDB.Storage.Page

open RyakDB.DataType
open RyakDB.Storage
open RyakDB.Storage.File

type Page =
    { GetVal: int32 -> DbType -> DbConstant
      SetVal: int32 -> DbConstant -> unit
      Read: BlockId -> unit
      Write: BlockId -> unit
      Append: string -> BlockId }

module Page =
    let ValSizeSize = 4

    let inline maxSize dbType =
        if dbType |> DbType.isFixedSize then dbType |> DbType.maxSize else ValSizeSize + (dbType |> DbType.maxSize)

    let inline size constant =
        let dbType = constant |> DbConstant.dbType
        if dbType |> DbType.isFixedSize
        then dbType |> DbType.maxSize
        else ValSizeSize + (constant |> DbConstant.size)

    let getVal contents offset dbType =
        let off, size =
            if dbType |> DbType.isFixedSize then
                offset, dbType |> DbType.maxSize
            else
                let bytes =
                    contents |> FileBuffer.get offset ValSizeSize

                offset + bytes.Length, System.BitConverter.ToInt32(System.ReadOnlySpan(bytes))

        contents
        |> FileBuffer.get off size
        |> DbConstant.fromBytes dbType

    let setVal fileMgr contents offset value =
        let bytes = value |> DbConstant.toBytes

        let off =
            if value |> DbConstant.dbType |> DbType.isFixedSize then
                offset
            else
                if offset
                   + ValSizeSize
                   + bytes.Length > fileMgr.BlockSize then
                    failwith
                        ("Page buffer overflow:offset="
                         + offset.ToString()
                         + ",size="
                         + bytes.Length.ToString())

                let sizebytes =
                    bytes.Length |> System.BitConverter.GetBytes

                contents |> FileBuffer.put offset sizebytes
                offset + sizebytes.Length

        contents |> FileBuffer.put off bytes

let newPage fileMgr =
    let contents = newFileBuffer fileMgr.BlockSize

    { GetVal = fun offset dbType -> lock contents (fun () -> Page.getVal contents offset dbType)
      SetVal = fun offset value -> lock contents (fun () -> Page.setVal fileMgr contents offset value)
      Read = fun blockId -> lock contents (fun () -> fileMgr.Read contents blockId)
      Write = fun blockId -> lock contents (fun () -> fileMgr.Write contents blockId)
      Append = fun fileName -> lock contents (fun () -> fileMgr.Append contents fileName) }
