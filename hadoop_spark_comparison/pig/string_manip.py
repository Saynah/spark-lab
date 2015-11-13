
@outputSchema("word:chararray")
def conv_to_30min(word):
    minute = word[11:13]
    modified_word = word[:11] + str(int(minute)/30*30).zfill(2) + "00"
    return modified_word

