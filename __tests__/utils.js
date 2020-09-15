function round(num, dec){
  if ((typeof num !== 'number') || (typeof dec !== 'number'))
    return false;

  const num_sign = num >= 0 ? 1 : -1;

  return (Math.round((num*Math.pow(10,dec))+(num_sign*0.0001))/Math.pow(10,dec)).toFixed(dec);
}

function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function calc_stats(values) {
  let count = 0;
  let sum = 0;
  let max = Number.NEGATIVE_INFINITY;
  let min = Number.POSITIVE_INFINITY;
  let vk = 0;
  let mean = 0;
  let std = 0;

  values.forEach(v => {
    let val = parseFloat(v);
    if (!isNaN(val)) {
      let oldMean = mean;
      count = count + 1;
      sum = sum + val;
      max = Math.max(max, val);
      min = Math.min(min, val);
      mean = sum / count;
      vk = vk + (val - mean) * (val - oldMean);
      std = Math.sqrt(vk / (count - 1));
    }
  });
  return {
    count,
    sum,
    min,
    max,
    mean,
    std
  }
}

function parseId(idval) {
  let [id, sequence] = (idval || '').split(',');
  if (id) id = parseInt(id);
  if (sequence) sequence = parseInt(sequence);
  return {id, sequence};
}

module.exports = {
  round,
  random,
  parseId,
  calc_stats
};
