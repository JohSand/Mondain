namespace Mondain
open MongoDB.Driver

open Mondain.Builders
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

    let explore2 (collection: IMongoCollection<Demo>) =
        findUpdate {
            for v in collection do
                where (v.Id = "")
                update (v.Name := "")
                update (v.Members ++ { Id = ""; Name = "test" })
                update (v.Members.removeWhere(fun i -> i.Id = "id"))
                update (v.Members.arrayFilter(fun i -> i.Id = "id").Name := "name")
                update (v.Version *= 5)
                returnDoc ReturnDocument.After
                select {| a = v.Name |}
        }

