const onlyDigits = (value) => String(value ?? "").replace(/\D/g, "");

const normalizeCpf = (value) => {
  const digits = onlyDigits(value);
  if (!digits || digits.length > 11) return "";
  return digits.padStart(11, "0");
};

const normalizeTelefone = (value) => {
  let digits = onlyDigits(value);
  if (!digits) return "";
  if (digits.length === 10) digits = `${digits.slice(0, 2)}9${digits.slice(2)}`;
  if (digits.length !== 11) return "";
  if (digits[2] !== "9") return "";
  return digits;
};

const normalizeNome = (value) =>
  String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();

const generateRandomTelefone = () => {
  while (true) {
    const value = String(Math.floor(Math.random() * (99999999999 - 11911111111 + 1)) + 11911111111);
    if (value.length === 11 && value[2] === "9") return value;
  }
};

const nowFileStamp = () => {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${hh}${mi}${ss}`;
};

module.exports = {
  onlyDigits,
  normalizeCpf,
  normalizeTelefone,
  normalizeNome,
  generateRandomTelefone,
  nowFileStamp,
};
