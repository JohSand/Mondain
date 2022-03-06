module Mondain.Tests

open System
open Xunit
open System.Linq.Expressions
open Mondain.Builders.Operators
open Mondain.Builders.Extensions
open Mondain.Builders.Core
open MongoDB.Bson
open MongoDB.Bson.Serialization
open MongoDB.Driver
open Mondain.TestModels

let serializerRegistry = BsonSerializer.SerializerRegistry

let inline render x =
    let serializer = serializerRegistry.GetSerializer<'T>()

    let bson =
        (^TRender: (member Render: IBsonSerializer<'T> -> 
                                   IBsonSerializerRegistry -> 
                                   Linq.LinqProvider -> 
                                   #BsonValue) 
                                   (x, serializer, serializerRegistry, Linq.LinqProvider.V2))

    bson.ToString()


type Xunit.Assert with
    static member inline RenderedEqual(expected: ^a, actual: ^b) =

        Assert.Equal(render expected, render actual)




type TestHelper =
    static member toExpression([<ReflectedDefinition>] exp: Expression<Func<'T, _>>) = exp



[<Fact>]
let ``My test`` () =
    let builder = UpdateDefinitionBuilder<_>()

    let expected =
        builder.Inc((fun (d: Demo) -> d.Version), 1)

    let exp =
        TestHelper.toExpression (fun (d: Demo) -> d.Version += 1)

    let struct (a, b) = createUpdate exp 0

    Assert.RenderedEqual(expected, a)



[<Fact>]
let ``My test2`` () =
    let exp =
        TestHelper.toExpression (fun d -> d.Members.arrayFilter(fun i -> i.Id = "").Dummy := obj())

    let struct (a, b) = createUpdate exp 1
    Assert.Equal("{ \"$set\" : { \"Members.$[filter_1].Dummy\" : { } } }", render a)
    let filter = Assert.Single(b)
    Assert.Equal("{ \"filter_1._id\" : \"\" }", render filter)


[<Fact>]
let ``My test3`` () =
    let builder = UpdateDefinitionBuilder<_>()

    let expected =
        builder.PullFilter((fun d -> d.Members), (fun (i: Inner) -> i.Id = ""))

    let exp =
        TestHelper.toExpression (fun d -> d.Members.removeWhere (fun i -> i.Id = ""))

    let struct (a, _) = createUpdateMulti exp 0

    Assert.RenderedEqual(expected, a)

[<Fact>]
let ``My test4`` () =
    let builder = UpdateDefinitionBuilder<_>()

    let expected =
        builder.PushEach((fun d -> d.Members), [ { Id = "i"; Name = "name"; Dummy = obj() } ])

    let a = [ { Id = "i"; Name = "name"; Dummy = obj()  } ]

    let exp =
        TestHelper.toExpression (fun d -> d.Members @@ [| { Id = "i"; Name = "name"; Dummy = obj()  } |])

    let struct (a, _) = createUpdateMulti exp 0
    let dbg = render a
    Assert.RenderedEqual(expected, a)

[<Fact>]
let ``My test5`` () =
    let a = { Id = "i"; Name = "name"; Dummy = obj()  }
    let builder = UpdateDefinitionBuilder<_>()
    let expected = builder.Pull((fun d -> d.Members), a)

    let exp =
        TestHelper.toExpression (fun d -> d.Members.removeItem a)

    let struct (a, _) = createUpdateMulti exp 0
    let dbg = render a
    Assert.RenderedEqual(expected, a)

[<Fact>]
let ``My test6`` () =
    let a = { Id = "i"; Name = "name"; Dummy = obj()  }
    let builder = UpdateDefinitionBuilder<_>()
    let expected = builder.PullAll((fun d -> d.Members), [ a ])

    let exp =
        TestHelper.toExpression (fun d -> d.Members.removeItems [| a |])

    let struct (a, _) = createUpdateMulti exp 0
    let dbg = render a
    Assert.RenderedEqual(expected, a)