use('mflix');
// 1. Especificar en la colección users las siguientes reglas de validación:
// El campo name (requerido) debe ser un string con un máximo de 30 caracteres,
// email (requerido) debe ser un string que matchee con la expresión regular:}
// "^(.*)@(.*)\\.(.{2,4})$" , password (requerido) debe ser un string con al menos 50 caracteres.
db.runCommand({
    collMod: "users",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["name", "email", "password"],
            properties: {
                name: {
                    bsonType: "string",
                    maxLength: 30,
                    description: "must be a string and is required"
                },
                email: {
                    bsonType: "string",
                    pattern: "^(.*)@(.*)\\.(.{2,4})$",
                    description: "must be a string and match the regular expression pattern"
                },
                password: {
                    bsonType: "string",
                    minLength: 50,
                    description: "must be a string and is required"
                }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "error"
});


// 2. Obtener metadata de la colección users que garantice que las reglas de validación
//    fueron correctamente aplicadas.
db.users.updateOne(  
    { "_id": ObjectId("59b99db4cfa9a34dcd7885b6")},  
    { $set: { name: null} } 
);

db.getCollectionInfos({name: "users"});


// 3. Especificar en la colección theaters las siguientes reglas de validación:
//    El campo theaterId (requerido) debe ser un int y location (requerido) debe ser un object con:
//        a. un campo address (requerido) que sea un object con campos street1, city, state
//           y zipcode todos de tipo string y requeridos
//        b. un campo geo (no requerido) que sea un object con un campo type, con valores posibles
//           “Point” o null y coordinates que debe ser una lista de 2 doubles
//    Por último, estas reglas de validación no deben prohibir la inserción o actualización
//    de documentos que no las cumplan sino que solamente deben advertir.
db.runCommand({
    collMod: "theaters",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["theaterId", "location"],
            properties: {
                theaterId: {
                    bsonType: "int",
                    description: "must be an int and is required"
                },
                location: {
                    bsonType: "object",
                    required: ["address"],
                    properties: {
                        address: {
                            bsonType: "object",
                            required: ["street1", "city", "state", "zipcode"],
                            properties: {
                                street1: {
                                    bsonType: "string",
                                    description: "must be a string and is required"
                                },
                                city: {
                                    bsonType: "string",
                                    description: "must be a string and is required"
                                },
                                state: {
                                    bsonType: "string",
                                    description: "must be a string and is required"
                                },
                                zipcode: {
                                    bsonType: "string",
                                    description: "must be a string and is required"
                                }
                            }
                        },
                        geo: {
                            bsonType: "object",
                            properties: {
                                type: {
                                    bsonType: ["string", "null"],
                                    enum: ["Point", null],
                                    description: "must be a string and is required"
                                },
                                coordinates: {
                                    bsonType: ["array", "null"],
                                    items: {
                                        bsonType: "double"
                                    },
                                    description: "must be an array of 2 doubles"
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "warn"
});


// 4. Especificar en la colección movies las siguientes reglas de validación:
//    El campo title (requerido) es de tipo string, year (requerido) int con mínimo en 1900 y máximo en 3000,
//    y que tanto cast, directors, countries, como genres sean arrays de strings sin duplicados.
//    Hint: Usar el constructor NumberInt() para especificar valores enteros a la hora de insertar documentos.
//    Recordar que mongo shell es un intérprete javascript y en javascript los literales numéricos
//    son de tipo Number (double).
db.runCommand({
    collMod: "movies",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["title", "year"],
            properties: {
                title: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                year: {
                    bsonType: "int",
                    minimum: 1900,
                    maximum: 3000,
                    description: "must be an int and is required"
                },
                cast: {
                    bsonType: "array",
                    uniqueItems: true,
                    items: {
                        bsonType: "string"
                    }
                },
                directors: {
                    bsonType: "array",
                    uniqueItems: true,
                    items: {
                        bsonType: "string"
                    }
                },
                countries: {
                    bsonType: "array",
                    uniqueItems: true,
                    items: {
                        bsonType: "string"
                    }
                },
                genres: {
                    bsonType: "array",
                    uniqueItems: true,
                    items: {
                        bsonType: "string"
                    }
                }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "error"
});

db.movies.insertOne({
    title: "Ejemplo de Película",
    year: NumberInt(2022),
    cast: ["Actor1", "Actor2"],
    directors: ["Director1"],
    countries: ["Country1"],
    genres: ["Genre1", "Genre2"]
});


// 5. Crear una colección userProfiles con las siguientes reglas de validación:
//    Tenga un campo user_id (requerido) de tipo “objectId”, un campo language (requerido)
//    con alguno de los siguientes valores [ “English”, “Spanish”, “Portuguese” ] y
//    un campo favorite_genres (no requerido) que sea un array de strings sin duplicados.
db.createCollection("userProfiles", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["user_id", "language"],
            properties: {
                user_id: {
                    bsonType: "objectId",
                    description: "must be an objectId and is required"
                },
                language: {
                    bsonType: "string",
                    enum: ["English", "Spanish", "Portuguese"],
                    description: "must be a string and is required"
                },
                favorite_genres: {
                    bsonType: "array",
                    uniqueItems: true,
                    items: {
                        bsonType: "string"
                    }
                }
            }
        }
    }
});


// 6. Identificar los distintos tipos de relaciones (One-To-One, One-To-Many) 
//    en las colecciones movies y comments. 
//    Determinar si se usó documentos anidados o referencias en cada relación y justificar la razón.

// One-To-Many en movies y comments. Se usaron referencias en ambas relaciones (commensts/movie_id).
// Se usó la estrategia de referencias porque la relación es One-To-Many y los comentarios son
// documentos relativamente grandes (no anidados) y se accede a ellos frecuentemente, por lo que
// no es conveniente anidarlos en movies.


// 7. Dado el diagrama de la base de datos shop junto con las queries más importantes. 
// Queries
//    1. Listar el id, titulo, y precio de los libros y sus categorías de un autor en particular 
//    2. Cantidad de libros por categorías
//    3. Listar el nombre y dirección entrega y el monto total (quantity * price)
//       de sus pedidos para un order_id dado.
// Debe crear el modelo de datos en mongodb aplicando las estrategias “Modelo de datos anidados”
// y Referencias. El modelo de datos debe permitir responder las queries de manera eficiente.

// 1. 
// Entidades: books - categories
// Relacion: One-To-Many
// Estrategia: Documentos anidados

// 2.
// Entidades: books - categories
// Relacion: One-To-Many
// Estrategia: Documentos anidados

// 3.
// Entidades: order_datails - orders
// Relacion: One-To-Many
// Estrategia: Referencias 

// Modelo de datos anidados
db.books.insertOne({
    _id: ObjectId("5a8086858b0beebeb69579cc"),
    title: "The Hobbit",
    author: "J.R.R. Tolkien",
    price: 10,
    categories: [
        {
            _id: ObjectId("5a8086858b0beebeb69579cd"),
            name: "Fantasy"
        },
        {
            _id: ObjectId("5a8086858b0beebeb69579ce"),
            name: "Adventure"
        }
    ]
}); 

// Query 1
db.books.filter({author: "J.R.R. Tolkien"}, {title: true, price: true, categories: true});

// Query 2
db.books.aggregate([
    {
        $unwind: "$categories"
    },
    {
        $group: {
            _id: "$categories.name",
            count: { $sum: 1 }
        }
    }
]);

// Referencias
db.orders.insertOne({
    _id: ObjectId("5a8086858b0beebeb69579cf"),
    delivery_name: "John Doe",
    delivery_address: "Fake Street 123",
    cc_name: "John Doe",
    cc_number: "123456789",
    cc_expiry: "01/01/2020",
});

db.order_details.insertMany([
    {
        _id: ObjectId("5a8086858b0beebeb69579d0"),
        order_id: ObjectId("5a8086858b0beebeb69579cf"),
        book_id: ObjectId("5a8086858b0beebeb69579cc"),
        quantity: 2,
        price: 10,
        author: "J.R.R. Tolkien",
        title: "The Hobbit",
    },
    {
        _id: ObjectId("5a8086858b0beebeb69579d1"),
        order_id: ObjectId("5a8086858b0beebeb69579cf"),
        book_id: ObjectId("5a8086858b0beebeb69579cd"),
        quantity: 1,
        price: 5,
        author: "J.R.R. Tolkien",
        title: "The Hobbit",
    }
]);

// Query 3
db.order_details.aggregate([
    {
        $match: { order_id: ObjectId("5a8086858b0beebeb69579cf") }
    },
    {
        $group: {
            _id: "$order_id",
            delivery_name: { $first: "$delivery_name" },
            delivery_address: { $first: "$delivery_address" },
            total: { $sum: { $multiply: ["$quantity", "$price"] } }
        }
    }
]);

