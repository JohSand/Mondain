namespace Mondain.TestModels

type Inner = { Id: string; Name: string; Dummy: obj }
[<CLIMutable>]
type Demo = { Id: string; Name: string; Version: int; Members: Inner seq }

