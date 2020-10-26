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

    let maxSize dbType =
        if dbType |> DbType.isFixedSize then DbType.maxSize dbType else ValSizeSize + DbType.maxSize dbType

    let size constant =
        let dbType = constant |> DbConstant.dbType
        if dbType |> DbType.isFixedSize then DbType.maxSize dbType else ValSizeSize + DbConstant.size constant

    let getVal contents offset dbType =
        let off, size =
            if dbType |> DbType.isFixedSize then
                offset, DbType.maxSize dbType
            else
                let bytes =
                    contents |> FileBuffer.get offset ValSizeSize

                offset + Array.length bytes, System.BitConverter.ToInt32(System.ReadOnlySpan(bytes))

        contents
        |> FileBuffer.get off size
        |> DbConstant.fromBytes dbType

    let setVal fileService contents offset value =
        let bytes = value |> DbConstant.toBytes

        let off =
            if value |> DbConstant.dbType |> DbType.isFixedSize then
                offset
            else
                if ValSizeSize
                   + offset
                   + Array.length bytes > fileService.BlockSize then
                    failwith
                        ("Page buffer overflow:offset="
                         + offset.ToString()
                         + ",size="
                         + bytes.Length.ToString())

                let sizebytes =
                    bytes
                    |> Array.length
                    |> System.BitConverter.GetBytes

                contents |> FileBuffer.put offset sizebytes
                offset + Array.length sizebytes

        contents |> FileBuffer.put off bytes

let newPage fileService =
    let contents = newFileBuffer fileService.BlockSize

    { GetVal = fun offset dbType -> lock contents (fun () -> Page.getVal contents offset dbType)
      SetVal = fun offset value -> lock contents (fun () -> Page.setVal fileService contents offset value)
      Read = fun blockId -> lock contents (fun () -> fileService.Read contents blockId)
      Write = fun blockId -> lock contents (fun () -> fileService.Write contents blockId)
      Append = fun fileName -> lock contents (fun () -> fileService.Append contents fileName) }
