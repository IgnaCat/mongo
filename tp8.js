use("mflix");
// 1. Cantidad de cines (theaters) por estado.
db.theaters.aggregate([{
    $group: {
        _id: "$location.address.state",
        count: {$sum: 1}
    }
},
{
    $sort: {
        count: -1
    }
}]);


// 2. Cantidad de estados con al menos dos cines (theaters) registrados.
db.theaters.aggregate([
{
    $group: {
        _id: "$location.address.state",
        count_state: { $count: {} },
    }
},
{
    $match: {count_state: {$gte: 2}}
},
{
    $sort: {
        count_state: 1
    }
}]);


// 3. Cantidad de películas dirigidas por "Louis Lumière". 
// Se puede responder sin pipeline de agregación, realizar ambas queries.
db.movies.find({"directors": {$all: ["Louis Lumière"]}}).count();
db.movies.aggregate([{
    $match: { "directors": {$all: ["Louis Lumière"]} }
},
{
    $count: "total"
}]);


// 4. Cantidad de películas estrenadas en los años 50 (desde 1950 hasta 1959).
// Se puede responder sin pipeline de agregación, realizar ambas queries.
db.movies.count({
    year: { $gte: 1950, $lte: 1959 },
});
db.movies.aggregate([{
    $match: { "year": {$gte: 1950, $lte: 1959} }
},
{
    $count: "total"
}]);


// 5. Listar los 10 géneros con mayor cantidad de películas 
// (tener en cuenta que las películas pueden tener más de un género).
// Devolver el género y la cantidad de películas. Hint: unwind puede ser de utilidad
db.movies.aggregate([{
    $unwind: "$genres"
},
{   $group: {
        _id: "$genres",
        count_genres: { $count: {} }}
},
{
    $sort: { count_genres: -1 }
},
{
    $limit: 10
}]);


// 6. Top 10 de usuarios con mayor cantidad de comentarios, mostrando Nombre, Email y Cantidad de Comentarios.
db.comments.aggregate([{
    $group: { 
        _id: {
            name: "$name",
            email: "$email",
          },
        count_comments: { $count: {} }}
},
{
    $sort: { count_comments: -1 }
},
{
    $limit: 10
},
{
    $project: {
        _id: 0,
        name: "$_id.name",
        email: "$_id.email",
        count_comments: 1
    }
}
]);


// 7. Ratings de IMDB promedio, mínimo y máximo por año de las películas estrenadas en los años 80 
// (desde 1980 hasta 1989), ordenados de mayor a menor por promedio del año.
db.movies.aggregate([{
    $match: { year: { $gte: 1980, $lte: 1989 } }
},
{
    $group: {
        _id: "$year",
        avg_rating: { $avg: "$imdb.rating" },
        min_rating: { $min: "$imdb.rating" },
        max_rating: { $max: "$imdb.rating" },
    }
},
{
    $sort: { avg_rating: -1 }
}]);


// 8. Título, año y cantidad de comentarios de las 10 películas con más comentarios.
db.movies.aggregate([
{
    $lookup: {
        from: "comments",
        localField: "_id",
        foreignField: "movie_id",
        as: "cmts"
    }   
},
{
    $addFields: { "cmts_count": { $size: "$cmts" }}
},
{
    $sort: { "cmts_count": -1 }
},
{
    $limit: 10
},
{
    $project: {
        _id: 0,
        title: 1,
        year: 1,
        cmts_count: 1
    }
}]);


// 9. Crear una vista con los 5 géneros con mayor cantidad de comentarios, 
// junto con la cantidad de comentarios.
db.createView("genres_comments", "movies", [
{
    $lookup: {
        from: "comments",
        localField: "_id",
        foreignField: "movie_id",
        as: "cmts"
    }
},
{
    $unwind: "$genres"
},
{
    $group: {
        _id: "$genres",
        count_cmts: { $sum: { $size: "$cmts" } }
    }
},
{
    $sort: { count_cmts: -1 }
},
{
    $limit: 5
}
]);


// 10. Listar los actores (cast) que trabajaron en 2 o más películas dirigidas por "Jules Bass".
// Devolver el nombre de estos actores junto con la lista de películas (solo título y año) dirigidas
// por “Jules Bass” en las que trabajaron. 
//     a. Hint1: addToSet
//     b. Hint2: {'name.2': {$exists: true}} permite filtrar arrays con al menos 2 elementos,
//        entender por qué.
//     c. Hint3: Puede que tu solución no use Hint1 ni Hint2 e igualmente sea correcta
db.movies.aggregate([
{
    $match: { "directors": {$all: ["Jules Bass"]} }
},
{
    $unwind: "$cast"
},
{
    $group: {
        _id: "$cast",
        count_genres: { $count: {} },
        movies: { $addToSet: { title: "$title", year: "$year" } }
    }
},
{
    $match: {
      "movies.1": {
        $exists: true,
      },
    },
},
{
    $project: {
        _id: 0,
        actor: "$_id",
        movies: 1,
        count_genres: 1,
    },
}]);



// 11. Listar los usuarios que realizaron comentarios durante el mismo mes de lanzamiento de la película
// comentada, mostrando Nombre, Email, fecha del comentario, título de la película, fecha de lanzamiento.
// HINT: usar $lookup con multiple condiciones 
db.comments.aggregate([
{
    $lookup: {
        from: "movies",
        let: { movie_id: "$movie_id", date: "$date" },
        pipeline: [
            {
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ["$_id", "$$movie_id"] },
                            { $eq: [{ $month: "$released" }, { $month: "$$date" }] },
                        ],
                    },
                },
            },
            {
                $project: {
                    title: 1,
                    released: 1,
                },
            },
        ],
        as: "movie",
    },
},
{
    $unwind: "$movie",
},
{
    $project: {
        _id: 0,
        name: 1,
        email: 1,
        date: 1,
        movie: 1,
    },
}]);


// 12. Listar el id y nombre de los restaurantes junto con su puntuación máxima, mínima y la suma total. Se puede asumir que el restaurant_id es único.
//     a. Resolver con $group y accumulators.
//     b. Resolver con expresiones sobre arreglos (por ejemplo, $sum) pero sin $group.
//     c. Resolver como en el punto b) pero usar $reduce para calcular la puntuación total.
//     d. Resolver con find.


// 13. Actualizar los datos de los restaurantes añadiendo dos campos nuevos. 
//     a. "average_score": con la puntuación promedio
//     b. "grade": con "A" si "average_score" está entre 0 y 13, 
//                 con "B" si "average_score" está entre 14 y 27 
//                 con "C" si "average_score" es mayor o igual a 28    
// Se debe actualizar con una sola query.
//     a. HINT1. Se puede usar pipeline de agregación con la operación update
//     b. HINT2. El operador $switch o $cond pueden ser de ayuda.



// agregar un campo nuevo que sea un arreglo de strings con valores numericos y luego convertirlo a int
db.movies.aggregate([
{
    $addFields: {
        "new_field": ["1", "2", "3"]
    }
},
{
    $addFields: {
        "new_field": {
            $map: {
                input: "$new_field",
                as: "item",
                in: { $toInt: "$$item" }
            }
        }
    }
}
]).pretty();