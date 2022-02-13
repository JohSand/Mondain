module Mondain.Builders.Update

open Abstractions

open MongoDB.Driver
open System
open System.Linq.Expressions

open Mondain.Builders.Core

type UpdateBuilder() =
    inherit BaseMongoBuilder()

    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: BaseMongoContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
        let struct (updateDef, filters) = createUpdate f 0
        let opts = UpdateOptions(ArrayFilters = filters)
        {| query = ctx; update = updateDef; options = opts |}


    [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
    member this.Update(
        ctx: MongoUpdateContext<'T>, 
        [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
        let struct (updateDef, filters) = createUpdate f (ctx.options.ArrayFilters :?> ArrayFilterDefinition[]).Length
        ctx.options.ArrayFilters <- [| yield! ctx.options.ArrayFilters; yield! filters |]
        {| ctx with update = Builders<'T>.Update.Combine(ctx.update, updateDef) |}

    [<CustomOperation("isUpsert", MaintainsVariableSpace = true)>]
    member _.IsUpsert(ctx: MongoUpdateContext<'T> , upsert: bool) =
        ctx.options.IsUpsert <- upsert
        ctx
