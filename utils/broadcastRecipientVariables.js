const extractRecipientVariables = (recipient = {}) => {
  const sources = [recipient, recipient?.data, recipient?.fullData, recipient?.attributes, recipient?.data?.fullData];
  for (const source of sources) {
    if (Array.isArray(source?.variables) && source.variables.length) return source.variables;
  }
  for (const source of [recipient?.fullData, recipient?.data, recipient?.attributes, recipient?.data?.fullData, recipient]) {
    const fields = Object.keys(source || {}).map((key) => {
      const match = key.trim().match(/^var\s*([1-9]\d*)$/i);
      return { key, index: match ? Number(match[1]) : 0 };
    }).filter(({ index }) => index > 0 && index <= 1000);
    if (!fields.length) continue;
    const values = Array(Math.max(...fields.map(({ index }) => index))).fill("");
    for (const { key, index } of fields) values[index - 1] = String(source[key] ?? "").trim();
    return values;
  }
  return [];
};

module.exports = { extractRecipientVariables };
