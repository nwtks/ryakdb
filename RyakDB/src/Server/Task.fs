namespace RyakDB.Server.Task

type Task = unit -> unit

type TaskManager = { RunTask: Task -> unit }

module TaskManager =
    let runTask (task: Task) =
        System.Threading.Tasks.Task.Factory.StartNew(task)
        |> ignore

    let newTaskManager () = { RunTask = runTask }
