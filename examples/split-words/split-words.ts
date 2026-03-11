import "t2-conduit";
deflift(split-words, (text: string): AsyncGenerator<string> => {
  async function*() {
    for (const raw of text.split(" ")) {
      const trimmed  = raw.trim();
      if (not(=(trimmed, ""))) {
        (yield trimmed);
      }
    }
  };
});
interface WordLength  { word: string; length: number }
deflift(measure-word-length, (token: string): WordLength => {
  ({
    word: token,
    length: token.length
  });
});
deflift(describe-word-length, (info: WordLength): string => {
  
}, `Word ${info.word} has ${info.length} characters.`);
