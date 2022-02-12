namespace Mondain

open MongoDB.Bson
open MongoDB.Driver
open System.Linq
open System.Linq.Expressions
open MongoDB.Bson.Serialization
open System.Collections.Concurrent
open System
open System.Reflection

[<AutoOpen>]
module private RenderCache =
    let constructorCache = ConcurrentDictionary<Type,ConstructorInfo>()
    let methodCache = ConcurrentDictionary<Type,MethodInfo>()
    ()
    let compile<'T> (mi: MethodInfo) =
        //let d = Delegate.CreateDelegate(typeof<int>, null, mi, true)
        
        let this = Expression.Parameter(mi.DeclaringType, "this")
        let parameters = [
            for parameter in mi.GetParameters() do
                Expression.Parameter(parameter.ParameterType, parameter.Name)
        ]
        let call = Expression.Call(this, mi, seq { for p in parameters do p :> Expression })
        Expression.Lambda<'T>(call, (this:: parameters) :> seq<_>).Compile()

        

type NamedArrayFilterDefinition(filterName: string, filterExpr: Expression) =
    inherit ArrayFilterDefinition()
    let ``type`` = filterExpr.Type.GenericTypeArguments.[0]
    override _.ItemType = ``type``
    //since we wont know the type at compile time
    member _.BaseFilterDefinitionType = typedefof<ExpressionFilterDefinition<_>>.MakeGenericType(``type``)
    //we could gain perf here by caching/compiling the methods instead of always using reflection.
    override this.Render(_s, r, p) =
        //this is ExpressionFilterDefinition<``type``>, but we don't know type at compile time.
        let ctor1 = constructorCache.GetOrAdd(filterExpr.GetType(), fun t -> this.BaseFilterDefinitionType.GetConstructor([| t |]) )

        let fd = ctor1.Invoke([| filterExpr |])

        //let fd = this.BaseFilterDefinitionType.GetConstructor([| filterExpr.GetType() |]).Invoke([| filterExpr |])
        //use the same Render as ExpressionFilterDefinition.
        let mi = methodCache.GetOrAdd(
                    this.BaseFilterDefinitionType,
                    fun t -> 
                        let registryType = typedefof<IBsonSerializer<_>>.MakeGenericType(``type``)
                        let mi = t.GetMethod("Render", [| registryType; typeof<IBsonSerializerRegistry>; typeof<Linq.LinqProvider> |])
                        mi
                    )

        let compiled = compile<Func<_,_,_,_,BsonDocument>> mi

        let wat = compiled.Invoke(fd, r.GetSerializer(``type``), r, p)
        //let mi = this.BaseFilterDefinitionType.GetMethod("Render", [| registryType; typeof<IBsonSerializerRegistry>; typeof<Linq.LinqProvider> |])
        let filter = mi
                         .Invoke(fd, [| r.GetSerializer(``type``); r; p |]) 
                         :?> BsonDocument
        //add the name of the filter
        filter.Names
            .Zip(filter.Values, (fun eName value -> (filterName + "." + eName, value)))   
            .ToDictionary(fst, snd)
            |> BsonDocument
