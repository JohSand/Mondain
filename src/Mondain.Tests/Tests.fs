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

let serializerRegistry = BsonSerializer.SerializerRegistry
let inline render x =
    let serializer =
        serializerRegistry.GetSerializer<'T>()
    let bson = ( ^TRender : (member Render: IBsonSerializer<'T> -> IBsonSerializerRegistry -> #BsonValue) (x,serializer,serializerRegistry) )
    bson.ToString()

let render2<'T> (x: ArrayFilterDefinition) =
    let serializer =
        serializerRegistry.GetSerializer<'T>()
    let bson = x.Render(serializer, serializerRegistry, Linq.LinqProvider.V2)
    bson.ToString()

type Xunit.Assert with
    static member inline RenderedEqual(expected: ^a, actual: ^b) =
        Assert.Equal(render expected, render actual)




type TestHelper =    
    static member toExpression([<ReflectedDefinition>] exp: Expression<Func<'T, _>>) =
        exp

type Inner = {Id: string; Name: string }
type Demo = { Id: string; Name: string; Version: int; Members: Inner list }

[<Fact>]
let ``My test`` () =
    let builder = UpdateDefinitionBuilder<_>()
    let expected = builder.Inc((fun (d: Demo) -> d.Version), 1)

    let exp = TestHelper.toExpression (fun (d: Demo) -> d.Version += 1)
    let struct (a, b) = createUpdate exp 0

    Assert.RenderedEqual(expected, a)



[<Fact>]
let ``My test2`` () =
    let exp = TestHelper.toExpression (fun d -> d.Members.arrayFilter(fun i -> i.Id = "").Name := "x")
    let struct (a, b) = createUpdate exp 1
    Assert.Equal("{ \"$set\" : { \"Members.$[filter_1].Name\" : \"x\" } }", render a)
    let filter = Assert.Single(b)
    Assert.Equal("{ \"filter_1._id\" : \"\" }", render2<Demo> filter)
