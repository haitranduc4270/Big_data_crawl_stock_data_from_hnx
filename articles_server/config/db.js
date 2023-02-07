const mongoose = require('mongoose');

async function connectDb() {
    try {
        await mongoose.connect('mongodb+srv://hai4270:hai4270@cluster-big-data.m420aoy.mongodb.net/big-data?retryWrites=true&w=majority');
        console.log('Connect successfully!!!');
    } catch (error) {
        console.log('Connect fail!!!');
    }
}
module.exports = { connectDb };




