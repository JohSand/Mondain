namespace Mondain
open MongoDB.Driver
open System
open System.Linq.Expressions
open System.Runtime.CompilerServices

open Mondain.Builders.Abstractions
open Mondain.Builders
open Mondain.Builders.Core
open System.Linq

module Explore =
    type Inner = {Id: string; Name: string }
    type Demo = { Id: string; Name: string; Version: int; Members: Inner list }

    let explore (eff: IMongoCollection<Demo>) =
        let shadow = [] @ []
        let t =
            find {
                for v in eff do
                    where (v.Id = "")
                    where (v.Members.Any(fun i -> i.Id = "asd"))
                    limit (10)
                    sortBy (v.Name)
                    //select ({| test = v.Id |})
                    //with_options ignore
            }
        ()

    let explore2 (eff: IMongoCollection<Demo>) =
        let t =
            findUpdate {
                for v in eff do
                    where (v.Id = "")
                    update (v.Name := "")
                    update (v.Members ++ { Id = ""; Name = "test" })
                    update (v.Members @@ [ { Id = ""; Name = "test" } ])
                    update (v.Members.arrayFilter(fun i -> i.Id = "id").Name := "name")
                    update (v.Version *= 5)
                    select {| a = v.Name |}
            }
        ()
