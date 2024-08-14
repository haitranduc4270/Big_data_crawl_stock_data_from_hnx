const mongoose = require('mongoose');

async function connectDb() {
    try {
        await mongoose.connect('mongodb+srv://hai4270:hai4270@dev.uov9yto.mongodb.net/?retryWrites=true&w=majority');
        console.log('Connect successfully!!!');
    } catch (error) {
        console.log('Connect fail!!!');
    }
}
module.exports = { connectDb };




