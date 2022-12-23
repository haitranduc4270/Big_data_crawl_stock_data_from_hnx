const fs = require('fs');
const reader = require('xlsx');

const xlsxReader = async () => {
  const file = reader.readFile('data/VietstockFinance_Du-lieu-doanh-nghiep_20221221-221917.xlsx');

  const buffer = reader.utils.sheet_to_json(file.Sheets[file.SheetNames[0]]);

  // const companies = buffer.map((company) => `${company['Mã CK']},${company['Tên công ty']},${company['Sàn CK']}\n`)
  // console.log(companies);
  //   fs.appendFileSync(`data/VietstockFinance_companies.csv`, 'Mã CK,Tên công ty,Sàn CK\n')
  
  // companies.forEach(company => {
  //   fs.appendFileSync(`data/VietstockFinance_companies.csv`, company);

  // })

  const companies = buffer.map((company) => `{"Mã CK":"${company['Mã CK']}","Tên công ty":"${company['Tên công ty']}","Sàn CK":"${company['Sàn CK']}"},\n`)
  companies.forEach(company => {
    fs.appendFileSync(`data/VietstockFinance_companies.json`, company);

  })


  // fs.writeFileSync('src/assets/tts-2022.json', JSON.stringify(teams, null, 4));
}


const main = async () => {
  await xlsxReader()
}

main();