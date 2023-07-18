const fs = require('fs');
const mongoose = require('mongoose');
const csvParser = require('csv-parser');

// Replace with your MongoDB connection string
const MONGODB_URI = 'mongodb+srv://DelwynMui:Delwyn16@cluster0.w69fom5.mongodb.net/Cluster0?retryWrites=true&w=majority';

// Create a Mongoose schema and model
const personSchema = new mongoose.Schema({
  name: String,
  domain: String,
  age: Number,
  city: String,
  age_range: String,
});

const Person = mongoose.model('Person', personSchema);

// Function to categorize age into ranges
function getAgeRange(age) {
  if (age >= 0 && age <= 10) return '0-10';
  else if (age >= 11 && age <= 20) return '11-20';
  else if (age >= 21 && age <= 30) return '21-30';
  else if (age >= 31 && age <= 40) return '31-40';
  else return '41+';
}

// Function to read the CSV file, insert data into MongoDB, and append age_range to the CSV
async function processData(csvPath) {
  const outputFilePath = 'output.csv';
  const writeStream = fs.createWriteStream(outputFilePath);
  writeStream.write('name,domain,age,age_range,city\n'); // Write header to the output CSV

  const data = [];

  fs.createReadStream(csvPath)
  .pipe(
    csvParser({
      trim: true,             // Enable trim to remove extra spaces
      skipEmptyLines: true,   // Skip empty lines in the CSV file
      mapHeaders: ({ header }) => header.trim(), // Trim header names to remove spaces
    })
  )
  .on('data', (row) => {
    console.log('Raw CSV Row:', row); // Display the raw CSV data

    const { name, domain, age, city } = row;
    const ageInt = parseInt(age.trim()); // Trim the age value before parsing

    if (isNaN(ageInt)) {
      console.error(`Error: Invalid age value for ${name}. Skipping this entry.`);
      return;
    }

      const age_range = getAgeRange(ageInt);

      console.log(`Processing: ${name}, ${domain}, ${age}, ${city}, ${age_range}`);

      data.push({ name, domain, age: ageInt, city, age_range });
    })
    .on('end', async () => {
      try {
        await mongoose.connect(MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true });
        console.log('Connected to MongoDB.');
        
        await Person.insertMany(data); // Insert data into MongoDB
        
        data.forEach((row) => {
          const csvRow = `${row.name},${row.domain},${row.age},${row.age_range},${row.city}`;
          writeStream.write(`${csvRow}\n`);
        });
        
        console.log('Data processing finished.');
        mongoose.disconnect();
      } catch (err) {
        console.error('Error saving data to MongoDB:', err);
        mongoose.disconnect();
      }
    })
    .on('error', (err) => {
      console.error('Error reading CSV:', err);
    });
}

const csvFilePath = 'age_range _original.csv'; // Replace with the path to your CSV file
processData(csvFilePath);
