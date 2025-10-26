from collections import Counter

def top_k_frequent(nums, k):
    # Step 1: Frequency dictionary (manual)
    freq_map = {}
    for num in nums:
        if num in freq_map:
            freq_map[num] +=1
        else:
            freq_map[num] = 1
    print("Frequency Map:", freq_map)

    #sorted_by_keys = dict(sorted(freq_map.items()))
    #sorted_items = sorted(freq_map.items(), key=lambda x: x[1], reverse=True)

   # print("sorted dictionary key",sorted_by_keys)
  #  print("sorted dictionary value",sorted_items)


    # Step 2: Convert to list of tuples (num, frequency)
    freq_list = []
    for key in freq_map:
        freq_list.append([key, freq_map[key]])
    print ("Frequency List before sort:", freq_list)
    freq_list.sort(key=lambda x: x[1], reverse=True)

    print("Sort Frequency List:", freq_list)
    result = []                          # create an empty list
    top_k_list = freq_list[0:k]          # get first k items
    print("Top K List:", top_k_list)
    for item in top_k_list:             # loop through each item
        result.append(item[0])          # add only the number (not frequency)
    print("Result:", result)
top_k_frequent([7,3,2,3,3,3,4,4,4,4,5,],3)