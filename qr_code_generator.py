import qrcode

# 오류 정정 수준 설정 (Level H)
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_H,  # 최대 30%까지 복구 가능
    box_size=10,
    border=4,
)

# QR 코드에 포함할 데이터
qr.add_data('https://www.youtube.com/@ColaCap')
qr.make(fit=True)

# QR 코드 생성
img = qr.make_image(fill='black', back_color='white')

# 이미지 저장
img.save('qr_code_output.png')