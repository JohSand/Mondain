namespace Mondain
open MongoDB.Driver
open MongoDB.Bson
open System
open System.Linq.Expressions
open System.Runtime.CompilerServices

type BaseMongoContext<'a> = 
    {
        collection: IMongoCollection<'a>
        filters: FilterDefinition<'a>
    }
type MongoQueryContext<'a> =
    {|
        query: BaseMongoContext<'a>
        options: FindOptions<'a>
    |}

type MongoQueryContext<'a, 'b> =
    {|
        query: BaseMongoContext<'a>
        options: FindOptions<'a, 'b>
    |}

type MongoUpdateContext<'a> =
    {|
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
        options: FindOneAndUpdateOptions<'a>
    |}

type MongoUpdateContext<'a, 'b> =
    {|
        query: BaseMongoContext<'a>
        update: UpdateDefinition<'a>
        options: FindOneAndUpdateOptions<'a, 'b>
    |}

module Builder =
    [<AbstractClass>]
    type BaseMongoBuilder() =
        member _.Yield(x: 'T) = ()

        member _.For(s: IMongoCollection<'T>, _: 'T -> unit) =
            s
        [<CustomOperation("where", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Where(s: IMongoCollection<'T>, [<ProjectionParameter; ReflectedDefinition>] f) =
            {
                collection = s
                filters = Builders<_>.Filter.Where(f)
            }

        [<CustomOperation("where", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Where(ctx: BaseMongoContext<'T>, [<ProjectionParameter; ReflectedDefinition>] f) =
            let clause = FilterDefinition.op_Implicit(predicate=f)
            { ctx with filters = ctx.filters &&& clause }

    type MongoQueryBuilder() =
        inherit BaseMongoBuilder()

        [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Limit(ctx: BaseMongoContext<'T>, limit) =
            {| query = ctx; options = (FindOptions<_>(Limit = limit)) |}


        [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Limit(ctx: MongoQueryContext<'T>, limit) : MongoQueryContext<'T> =
            ctx.options.Limit <- limit
            ctx

        [<CustomOperation("limit", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Limit(ctx: MongoQueryContext<'T, 'TProj>, limit) : MongoQueryContext<'T, 'TProj> =
            ctx.options.Limit <- limit
            ctx


        [<CustomOperation("skip", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Skip(ctx: MongoQueryContext<'T>, skip) =
            ctx.options.Skip <- skip
            ctx

        [<CustomOperation("sort", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Sort(ctx: MongoQueryContext<'T>, text: string) =
            let sort = Builders<_>.Sort.MetaTextScore(text)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx

        [<CustomOperation("sortBy", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.SortBy(ctx: MongoQueryContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
            let sort = Builders<_>.Sort.Ascending(sort)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx

        [<CustomOperation("sortByDesc", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.SortDesc(ctx: MongoQueryContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
            let sort = Builders<_>.Sort.Descending(sort)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx


        [<CustomOperation("select")>]
        member _.Projection(
            ctx: BaseMongoContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
                : MongoQueryContext<_,_> =
            let proj = Builders<'T>.Projection.Expression<'TProj>(p)
            let opts = FindOptions<_,_>(Projection = proj)                
            {| query = ctx; options = opts |}
                
        [<CustomOperation("select")>]
        member _.Projection(
            ctx: MongoQueryContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
                : MongoQueryContext<_,_> =
            let proj = Builders<'T>.Projection.Expression<'TProj>(p)
            let opts = FindOptions<_,_>(Projection = proj, Limit = ctx.options.Limit, Skip = ctx.options.Skip, Sort = ctx.options.Sort)                
            {| ctx with options = opts |}

        [<CustomOperation("with_options")>]
        member _.WithOptions(ctx: BaseMongoContext<'T>, fOpts) =    
            let opts = FindOptions<_>()
            do fOpts (opts)
            {| query = ctx; options = opts |}

        [<CustomOperation("with_options")>]
        member _.WithOptions(ctx: MongoQueryContext<'T>, fOpts) = 
            do fOpts (ctx.options)
            ctx

        [<CustomOperation("with_options")>]
        member _.WithOptions(ctx: MongoQueryContext<'T, 'TProj>, fOpts) = 
            do fOpts (ctx.options)
            ctx



    [<Extension>]
    type SeqExtensions =
        [<Extension>]
        static member arrayFilter(_s: System.Collections.Generic.IEnumerable<'T>, _i: Func<'T, bool>): 'T =
            failwith "This is only intended to be used as part of an array filter in the mongo DSL."

    [<Struct; NoComparison;NoEquality>]
    type Update<'a> = 
        private 
        | SetUpdate of Set:'a
        | IncrUpdate of Incr:'a
    
    [<AutoOpen>]
    module Operators =
        let (:=) (_: 'a) (a: 'a) = SetUpdate a
    
        let (+=) (_: 'a) (a: 'a) = IncrUpdate a


    let createDefinition(e: Expression<Func<'T, Update<'a>>>) : struct (FieldDefinition<'T, 'a> * Update<'a> * _) =
        match e.Body with
        | :? MethodCallExpression as mce ->
            let lhs = mce.Arguments.[0]
            let rhs = mce.Arguments.[1] :?> ConstantExpression
            let value = rhs.Value :?> 'a
            let update = 
                match mce.Method.Name with
                | "op_ColonEquals" -> SetUpdate value
                | "op_AdditionAssignment" -> IncrUpdate value
                | _ -> failwith "stop creating wrapper types in unintended ways."

            let (setExpression, filters) = ExpressionParsing.checkArrayFilters(lhs)

            let fieldDef = 
                if filters.Length > 0 then
                    FieldDefinition<_, _>.op_Implicit setExpression
                else
                    Expression.Lambda<Func<_, _>>(lhs, e.Parameters.[0]) 
                    |> ExpressionFieldDefinition<_, _> 
                    :> FieldDefinition<_,_>

            struct (fieldDef, update, filters)
        | _ -> failwith "stop creating wrapper types in unintended ways."

    let createUpdate f =
        let struct (update, value, filters) = createDefinition(f)
        let builder = Builders<'T>.Update
        
        let updateDef = 
            match value with
            | SetUpdate value -> 
                builder.Set(update, value)
            | IncrUpdate value -> 
                builder.Inc(update, value)

        struct (updateDef, filters)


    type MongoUpdateBuilder() =
        inherit BaseMongoBuilder()

        [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member this.Update(
            ctx: BaseMongoContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
            let struct (updateDef, filters) = createUpdate f
            let opts = FindOneAndUpdateOptions<_>(ArrayFilters = filters)
            {| query = ctx; update = updateDef; options = opts |}


        [<CustomOperation("update", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member this.Update(
            ctx: MongoUpdateContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] f: Expression<Func<'T, Update<'a>>>) =
            let struct (updateDef, filters) = createUpdate f     
            ctx.options.ArrayFilters <- [| yield! ctx.options.ArrayFilters; yield! filters |]
            {| ctx with update = Builders<'T>.Update.Combine(ctx.update, updateDef) |}

        [<CustomOperation("returnDoc", MaintainsVariableSpace = true)>]
        member _.ReturnDocument(ctx: MongoUpdateContext<'T> , ret) =
            ctx.options.ReturnDocument <- ret
            ctx

        [<CustomOperation("isUpsert", MaintainsVariableSpace = true)>]
        member _.IsUpsert(ctx: MongoUpdateContext<'T> , upsert: bool) =
            ctx.options.IsUpsert <- upsert
            ctx

        [<CustomOperation("sort", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.Sort(ctx: MongoUpdateContext<'T>, text: string) =
            let sort = Builders<_>.Sort.MetaTextScore(text)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx

        [<CustomOperation("sortBy", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.SortBy(ctx: MongoUpdateContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
            let sort = Builders<_>.Sort.Ascending(sort)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx

        [<CustomOperation("sortByDesc", MaintainsVariableSpace = true, AllowIntoPattern = true)>]
        member _.SortDesc(ctx: MongoUpdateContext<'T>, [<ProjectionParameter; ReflectedDefinition>] sort: Expression<_>) =
            let sort = Builders<_>.Sort.Descending(sort)            
            match ctx.options.Sort with
            | null -> ctx.options.Sort <- sort
            | s -> ctx.options.Sort <- Builders<_>.Sort.Combine(sort, s)
            ctx

        [<CustomOperation("select")>]
        member _.Projection(
            ctx: MongoUpdateContext<'T>, 
            [<ProjectionParameter; ReflectedDefinition>] p: Expression<Func<'T, 'TProj>>)
                : MongoUpdateContext<_,_> =
            let proj = Builders<'T>.Projection.Expression<'TProj>(p)
            //todo copy all options
            let opts = FindOneAndUpdateOptions<_,_>(Projection = proj, Sort = ctx.options.Sort)                
            {| ctx with options = opts |}

        member _.Run(ctx: MongoUpdateContext<'T>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)

        member _.Run(ctx: MongoUpdateContext<'T, 'TProj>) =
            let filter = ctx.query.filters
            let update = ctx.update
            let options = ctx.options
            ctx.query.collection.FindOneAndUpdateAsync(filter, update, options)

open Builder
module Explore =
    type Demo = { Id: string; Name: string; Version: int }

    let m = MongoQueryBuilder()
    let explore (eff: IMongoCollection<Demo>) =
        let t =
            m {
                for v in eff do
                    where (v.Id = "")
                    where (v.Name = "")
                    limit (10)
                    sortBy (v.Name)
                    //select ({| test = v.Id |})
                    //with_options ignore
            }
        ()

    let m2 = MongoUpdateBuilder()
    let explore2 (eff: IMongoCollection<Demo>) =
        let t =
            m2 {
                for v in eff do
                    where (v.Id = "")
                    where (v.Name = "")
                    update (v.Version += 2)
                    select {| a = v.Name |}
            }
        ()
