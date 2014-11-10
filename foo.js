
console.log('Started');

setTimeout(function() {
    console.log('Throwing error');
    throw new Error('Something went wrong!');
}, 5000);
