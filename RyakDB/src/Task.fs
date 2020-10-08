module RyakDB.Task

type Task = unit -> unit

type TaskManager =
    { RunTask: Task -> unit
      CancelTasks: unit -> unit }

module TaskManager =
    let tokenSource =
        new System.Threading.CancellationTokenSource()

    let runTask (task: Task) =
        System.Threading.Tasks.Task.Run(task, tokenSource.Token)
        |> ignore

    let cancelTasks () = tokenSource.Cancel()

let newTaskManager () =
    { RunTask = TaskManager.runTask
      CancelTasks = TaskManager.cancelTasks }
