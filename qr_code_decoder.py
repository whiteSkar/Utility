from pyzbar.pyzbar import decode
from PIL import Image

# QR 코드 이미지 불러오기
image = Image.open('qr_code_output - Copy - Copy.png')

# QR 코드 디코딩
decoded_data = decode(image)

# QR 코드 내의 데이터 출력
for data in decoded_data:
    print(data.data.decode('utf-8'))