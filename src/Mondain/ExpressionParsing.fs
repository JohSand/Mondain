module Mondain.ExpressionParsing 


open MongoDB.Bson.Serialization

open System
open System.Collections.Concurrent
open System.Linq.Expressions
open System.Reflection

let serializerRegistry = BsonSerializer.SerializerRegistry

[<AutoOpen>]
module internal RenderCache =
    let constructorCache = ConcurrentDictionary<Type, Func<obj[], obj>>()
    let renderMethodCache = ConcurrentDictionary<Type, Func<obj, obj[], obj>>()
    let updatorMethodCache = ConcurrentDictionary<string, Func<obj, obj[], obj>>()
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
    let compileMethod (mi: MethodInfo) =
        let args = Expression.Parameter(typeof<obj[]>, "args")

        let paramsExprs = 
            [| for p in mi.GetParameters() do p.ParameterType |] 
            |> Array.mapi (fun i par ->
                let arg = Expression.ArrayIndex(args, Expression.Constant(i, typeof<int>))
                Expression.Convert(arg, par) :> Expression
            )

        let target = Expression.Parameter(typeof<obj>, "target")

        let call = 
            if mi.IsStatic then
                Expression.Call( mi, paramsExprs)
            else                
                Expression.Call(Expression.Convert(target, mi.DeclaringType), mi, paramsExprs)

        

        let convert = Expression.Convert(call, typeof<obj>)
        Expression.Lambda(convert, target, args).Compile() :?> Func<obj, obj[], obj>


let private resolve (mex: MemberExpression) =
    let memberName = mex.Member.Name
    match serializerRegistry.GetSerializer(mex.Member.ReflectedType) with
    | :? IBsonDocumentSerializer as ser ->
        match ser.TryGetMemberSerializationInfo(memberName) with
        | true, resx -> resx.ElementName
        | false, _ -> memberName
    | _ -> memberName

let rec internal parseMember 
        (body: Expression)  
        (pathFragments: _ list) 
        filters 
        (counter: int) 
        (f)
            =
    match body with
    | :? LambdaExpression as l ->
        parseMember l.Body pathFragments filters counter f
    | :? MemberExpression as mex ->
        parseMember mex.Expression (resolve mex :: pathFragments) filters counter f
    | :? MethodCallExpression as mce when (mce.Method.Name = "arrayFilter") ->
        match mce.Arguments[0], mce.Arguments[1] with
        | :? MemberExpression as mex, (:? LambdaExpression as lex) ->        
            let randomName = $"filter_{counter}"
            let counter = counter + 1
            let filters = (f randomName lex) :: filters

            let fragments = resolve mex :: $"$[{randomName}]" :: pathFragments

            parseMember mex.Expression fragments filters counter f
        | _ -> (pathFragments, filters)

    | _ -> (pathFragments, filters)



let rec construct (expr: Expression) =
    match expr with
    | :? ConstantExpression as rhs ->
        rhs.Value   
    | :? NewExpression as rhs ->
        let args = [| for a in rhs.Arguments -> construct a |]
        constructorCache
            .GetOrAdd(rhs.Type, (fun _ ci -> compileCtor ci), rhs.Constructor)
            .Invoke(args)
    | :? NewArrayExpression as rhs ->
        let args = [| for a in rhs.Expressions -> construct a |]
        args
    | :? MethodCallExpression as rhs ->
        //todo fastera
        if not rhs.Method.IsStatic then
            let args = [| for a in rhs.Arguments |> Seq.skip 1 -> construct a |]
            rhs.Method.Invoke(construct rhs.Arguments[0], args)
        else
            let args = [| for a in rhs.Arguments -> construct a |]
            rhs.Method.Invoke(null, args)
    | _ -> Unchecked.defaultof<_>