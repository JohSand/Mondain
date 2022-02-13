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
    let constructorCache = ConcurrentDictionary<Type, Func<obj[], obj>>()
    let methodCache = ConcurrentDictionary<Type, Func<obj, obj[], obj>>()
    
    let compileCtor (ctor: ConstructorInfo) =
        let args = Expression.Parameter(typeof<obj[]>, "args")

        let paramsExprs = 
            [| for p in ctor.GetParameters() do p.ParameterType |] 
            |> Array.mapi (fun i par ->
                let arg = Expression.ArrayIndex(args, Expression.Constant(i, typeof<int>))
                Expression.Convert(arg, par) :> Expression
            )
        let newExpr = Expression.New(ctor, paramsExprs)
        let convert = Expression.Convert(newExpr, typeof<obj>)
        Expression.Lambda(convert, args).Compile() :?> Func<obj[], obj>
        
    //we cannot compile it typed, since we don't have enough type info. Compile in casts
    let compileMethod (``type``: Type) (mi: MethodInfo) =
        let args = Expression.Parameter(typeof<obj[]>, "args")

        let paramsExprs = 
            [| for p in mi.GetParameters() do p.ParameterType |] 
            |> Array.mapi (fun i par ->
                let arg = Expression.ArrayIndex(args, Expression.Constant(i, typeof<int>))
                Expression.Convert(arg, par) :> Expression
            )

        let target = Expression.Parameter(typeof<obj>, "target")
        let call = Expression.Call(Expression.Convert(target, ``type``), mi, paramsExprs)
        let convert = Expression.Convert(call, typeof<obj>)
        Expression.Lambda(convert, target, args).Compile() :?> Func<obj, obj[], obj>
        

type NamedArrayFilterDefinition(filterName: string, filterExpr: Expression) =
    inherit ArrayFilterDefinition()
    let ``type`` = filterExpr.Type.GenericTypeArguments.[0]
    override _.ItemType = ``type``
    //since we wont know the type at compile time
    member _.BaseFilterDefinitionType = typedefof<ExpressionFilterDefinition<_>>.MakeGenericType(``type``)
    
    override this.Render(_s, r, p) =
        //this is ExpressionFilterDefinition<``type``>, but we don't know type at compile time.
        let s = r.GetSerializer(``type``)

        let filterDef =
            constructorCache
                .GetOrAdd(
                    this.BaseFilterDefinitionType,
                    fun (t: Type) -> t.GetConstructor(types = [| filterExpr.GetType() |]) |> compileCtor
                )
                .Invoke([| filterExpr |])

        //use the same Render as ExpressionFilterDefinition.
        let filter =
            methodCache
                .GetOrAdd(
                    this.BaseFilterDefinitionType,
                    fun t -> t.GetMethod("Render", [| s.GetType(); r.GetType(); p.GetType() |]) |> compileMethod t
                )
                .Invoke(filterDef, [| s; r; p |]) :?> BsonDocument

        //add the name of the filter
        filter.Names
            .Zip(filter.Values, (fun eName value -> (filterName + "." + eName, value)))   
            .ToDictionary(fst, snd)
            |> BsonDocument
