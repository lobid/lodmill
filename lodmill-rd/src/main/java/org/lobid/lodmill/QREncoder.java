package org.lobid.lodmill;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;

final class QREncoder {
	final static String fileType = "png";
	final static String fileSuffix = "_contactqr";
	static private Map<EncodeHintType, Object> hintMap = new HashMap<>();
	QRCodeWriter qrCodeWriter;
	BufferedImage image;
	Graphics2D graphics;

	public QREncoder() {
		qrCodeWriter = new QRCodeWriter();
		hintMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L);
	}

	/**
	 * 
	 * @param filePath Directory where the qr images should be saved to
	 * @param qrCodeText The text of the qr code
	 * @param x the width and height of the qr image
	 * @throws WriterException If problems while writing occur
	 * @throws IOException If problems while writing occur
	 */
	public void createQRImage(final String filePath, final String qrCodeText,
			final int x) throws WriterException, IOException {
		final File qrFile = new File(filePath + fileSuffix + "." + fileType);
		// Create the ByteMatrix for the QR-Code that encodes the given String
		final BitMatrix byteMatrix =
				qrCodeWriter.encode(qrCodeText, BarcodeFormat.QR_CODE, x, x, hintMap);
		// Make the BufferedImage that hold the QRCode
		image = new BufferedImage(x, x, BufferedImage.TYPE_INT_RGB);
		image.createGraphics();
		graphics = (Graphics2D) image.getGraphics();
		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, x, x);
		// Paint and save the image using the ByteMatrix
		graphics.setColor(Color.BLACK);
		for (int i = 0; i < x; i++) {
			for (int j = 0; j < x; j++) {
				if (byteMatrix.get(i, j)) {
					graphics.fillRect(i, j, 1, 1);
				}
			}
		}
		ImageIO.write(image, fileType, qrFile);
	}
}