lst = [1,2,3,4,5]
lst_2 = [3,4,5,7,8,9]
# lst3 = lst + lst_2
# print(list(set(lst3)))


# for i in lst_2:
#     if i not in lst:
#         lst.append(i)
#
# print(list(lst))

str1 = "baaa"

def palindrome(str):
    for i in range(0,int(len(str)/2)):
        if(str[i] != str[len(str)-i-1]):
            return False
        else:
            return True

print(palindrome(str1))








