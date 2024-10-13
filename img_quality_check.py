import imquality.brisque as brisque
import PIL.Image
import os
import shutil

dir_path = "C:\\Users"
high_quality_path = os.path.join(dir_path, 'high_quality')
# lower scores means better quality. However, my high quality photos are also getting high scores, too.
# This tool is not reliable.
high_quality_score = 20
img_extensions = {'.png', '.jpg', '.jpeg'}

os.makedirs(high_quality_path, exist_ok=True)

dir_contents = os.listdir(dir_path)
for dir_content_name in list(dir_contents):
    dir_content_path = os.path.join(dir_path, dir_content_name)
    if not os.path.isfile(dir_content_path):
        continue

    file_name = dir_content_name
    file_path = dir_content_path
    file_name_without_extension, extension = os.path.splitext(file_name)
    if extension.lower() not in img_extensions:
        continue

    score_file_name = f'{file_name_without_extension}.txt'
    score_file_path = os.path.join(dir_path, score_file_name)
    high_quality_path_score_file_path = os.path.join(high_quality_path, score_file_name)
    if os.path.exists(score_file_path) or os.path.exists(high_quality_path_score_file_path):
        continue

    img = PIL.Image.open(file_path)
    score = brisque.score(img)
    print(f'{file_name}: {score}')

    if score >= high_quality_score:
        shutil.move(file_path, high_quality_path)
        score_file_path = high_quality_path_score_file_path

    with open(score_file_path, 'w') as score_file:
        score_file.write(str(score))

print('Done')